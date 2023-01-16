package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/filecoin-project/lotus/node/repo"
	"github.com/libp2p/go-libp2p/core/crypto"
	host "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/fx"

	dst "github.com/filecoin-project/dagstore"
	lotus_config "github.com/filecoin-project/lotus/node/config"

	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/markets/idxprov"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/hashicorp/go-multierror"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/ipni/index-provider/engine/xproviders"
	"github.com/ipni/index-provider/metadata"

	"github.com/filecoin-project/boost/db"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/ipfs/go-cid"
	provider "github.com/ipni/index-provider"
)

var log = logging.Logger("index-provider-wrapper")
var shardRegMarker = ".boost-shard-registration-complete"
var defaultDagStoreDir = "dagstore"

type Wrapper struct {
	cfg         lotus_config.DAGStoreConfig
	enabled     bool
	dealsDB     *db.DealsDB
	legacyProv  lotus_storagemarket.StorageProvider
	prov        provider.Interface
	dagStore    *dagstore.Wrapper
	meshCreator idxprov.MeshCreator
	h           host.Host
	// bitswapEnabled records whether to announce bitswap as an available
	// protocol to the network indexer
	bitswapEnabled bool
	// when booster bitswap is exposed on a public address, extendedProvider
	// holds the information needed to announce that multiaddr to the network indexer
	// as the provider of bitswap
	extendedProvider *xproviders.Info
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface, dagStore *dagstore.Wrapper,
	meshCreator idxprov.MeshCreator) (*Wrapper, error) {

	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface, dagStore *dagstore.Wrapper,
		meshCreator idxprov.MeshCreator) (*Wrapper, error) {
		if cfg.DAGStore.RootDir == "" {
			cfg.DAGStore.RootDir = filepath.Join(r.Path(), defaultDagStoreDir)
		}

		_, isDisabled := prov.(*DisabledIndexProvider)

		// bitswap is enabled if there is a bitswap peer id
		bitswapEnabled := cfg.Dealmaking.BitswapPeerID != ""

		// setup bitswap extended provider if there is a public multi addr for bitswap
		var ep *xproviders.Info
		if bitswapEnabled && len(cfg.Dealmaking.BitswapPublicAddresses) > 0 {
			// marshal bitswap metadata
			meta := metadata.Default.New(metadata.Bitswap{})
			mbytes, err := meta.MarshalBinary()
			if err != nil {
				return nil, err
			}
			// we need the private key for bitswaps peerID in order to announce publicly
			keyFile, err := os.ReadFile(cfg.Dealmaking.BitswapPrivKeyFile)
			if err != nil {
				return nil, err
			}
			privKey, err := crypto.UnmarshalPrivateKey(keyFile)
			if err != nil {
				return nil, err
			}
			// setup an extended provider record, containing the booster-bitswap multi addr,
			// peer ID, private key for signing, and metadata
			ep = &xproviders.Info{
				ID:       cfg.Dealmaking.BitswapPeerID,
				Addrs:    cfg.Dealmaking.BitswapPublicAddresses,
				Priv:     privKey,
				Metadata: mbytes,
			}
		}

		w := &Wrapper{
			h:                h,
			dealsDB:          dealsDB,
			legacyProv:       legacyProv,
			prov:             prov,
			dagStore:         dagStore,
			meshCreator:      meshCreator,
			cfg:              cfg.DAGStore,
			bitswapEnabled:   bitswapEnabled,
			extendedProvider: ep,
			enabled:          !isDisabled,
		}
		// announce all deals on startup in case of a config change
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					_ = w.AnnounceExtendedProviders(ctx)
				}()
				return nil
			},
		})
		return w, nil
	}
}

func NewWrapperNoLegacy() func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	prov provider.Interface, dagStore *dagstore.Wrapper, meshCreator idxprov.MeshCreator) (*Wrapper, error) {
	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		prov provider.Interface, dagStore *dagstore.Wrapper, meshCreator idxprov.MeshCreator) (*Wrapper, error) {
		nodeCfg, err := r.Config()
		if err != nil {
			return nil, err
		}
		cfg, ok := nodeCfg.(*config.Boost)
		if !ok {
			return nil, err
		}

		w := NewWrapper(cfg)
		return w(lc, h, r, dealsDB, nil, prov, dagStore, meshCreator)
	}
}

func (w *Wrapper) Enabled() bool {
	return w.enabled
}

func (w *Wrapper) AnnounceExtendedProviders(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}
	// for now, only generate an indexer provider announcement if bitswap announcements
	// are enabled -- all other graphsync announcements are context ID specific
	if !w.bitswapEnabled {
		return nil
	}

	// build the extended providers announcement
	adBuilder := xproviders.NewAdBuilder(w.h.ID(), w.h.Peerstore().PrivKey(w.h.ID()), w.h.Addrs())
	// if we're exposing bitswap publicly, we announce bitswap as an extended provider. If we're not
	// we announce it as metadata on the main provider
	if w.extendedProvider != nil {
		adBuilder.WithExtendedProviders(*w.extendedProvider)
	} else {
		meta := metadata.Default.New(metadata.Bitswap{})
		mbytes, err := meta.MarshalBinary()
		if err != nil {
			return err
		}
		adBuilder.WithMetadata(mbytes)
	}
	last, _, err := w.prov.GetLatestAdv(ctx)
	if err != nil {
		return err
	}
	adBuilder.WithLastAdID(last)
	ad, err := adBuilder.BuildAndSign()
	if err != nil {
		return err
	}

	// publish the extended providers announcement
	_, err = w.prov.Publish(ctx, *ad)
	return err
}

func (w *Wrapper) IndexerAnnounceAllDeals(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}

	if w.legacyProv != nil {
		log.Info("will announce all Markets deals to Indexer")
		err := w.legacyProv.AnnounceAllDealsToIndexer(ctx)
		if err != nil {
			log.Warnw("some errors while announcing legacy deals to index provider", "err", err)
		}
		log.Infof("finished announcing markets deals to indexer")
	}

	log.Info("will announce all Boost deals to Indexer")
	deals, err := w.dealsDB.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list deals: %w", err)
	}

	shards := make(map[string]struct{})
	var nSuccess int
	var merr error

	for _, d := range deals {
		// filter out deals that will announce automatically at a later
		// point in their execution, as well as deals that are not processing at all
		// (i.e. in an error state or expired)
		// (note technically this is only one check point state IndexedAndAnnounced but is written so
		// it will work if we ever introduce additional states between IndexedAndAnnounced & Complete)
		if d.Checkpoint < dealcheckpoints.IndexedAndAnnounced || d.Checkpoint >= dealcheckpoints.Complete {
			continue
		}

		if _, err := w.AnnounceBoostDeal(ctx, d); err != nil {
			// don't log already advertised errors as errors - just skip them
			if !errors.Is(err, provider.ErrAlreadyAdvertised) {
				merr = multierror.Append(merr, err)
				log.Errorw("failed to announce boost deal to Index provider", "dealId", d.DealUuid, "err", err)
			}
			continue
		}
		shards[d.ClientDealProposal.Proposal.PieceCID.String()] = struct{}{}
		nSuccess++
	}

	log.Infow("finished announcing boost deals to index provider", "number of deals", nSuccess, "number of shards", len(shards))
	return merr
}

func (w *Wrapper) Start(ctx context.Context) {
	// re-init dagstore shards for Boost deals if needed
	if _, err := w.DagstoreReinitBoostDeals(ctx); err != nil {
		log.Errorw("failed to migrate dagstore indices for Boost deals", "err", err)
	}

	w.prov.RegisterMultihashLister(func(ctx context.Context, pid peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		provideF := func(pieceCid cid.Cid) (provider.MultihashIterator, error) {
			ii, err := w.dagStore.GetIterableIndexForPiece(pieceCid)
			if err != nil {
				return nil, fmt.Errorf("failed to get iterable index: %w", err)
			}

			mhi, err := provider.CarMultihashIterator(ii)
			if err != nil {
				return nil, fmt.Errorf("failed to get mhiterator: %w", err)
			}
			return mhi, nil
		}

		// convert context ID to proposal Cid
		proposalCid, err := cid.Cast(contextID)
		if err != nil {
			return nil, fmt.Errorf("failed to cast context ID to a cid")
		}

		// go from proposal cid -> piece cid by looking up deal in boost and if we can't find it there -> then markets
		// check Boost deals DB
		pds, boostErr := w.dealsDB.BySignedProposalCID(ctx, proposalCid)
		if boostErr == nil {
			pieceCid := pds.ClientDealProposal.Proposal.PieceCID
			return provideF(pieceCid)
		}

		// check in legacy markets
		var legacyErr error
		if w.legacyProv != nil {
			md, legacyErr := w.legacyProv.GetLocalDeal(proposalCid)
			if legacyErr == nil {
				return provideF(*md.Ref.PieceCid)
			}
		}

		return nil, fmt.Errorf("failed to look up deal in Boost, err=%s and Legacy Markets, err=%s", boostErr, legacyErr)
	})
}

func (w *Wrapper) AnnounceBoostDeal(ctx context.Context, pds *types.ProviderDealState) (cid.Cid, error) {
	if !w.enabled {
		return cid.Undef, errors.New("cannot announce deal: index provider is disabled")
	}

	// Announce deal to network Indexer
	protocols := []metadata.Protocol{
		&metadata.GraphsyncFilecoinV1{
			PieceCID:      pds.ClientDealProposal.Proposal.PieceCID,
			FastRetrieval: pds.FastRetrieval,
			VerifiedDeal:  pds.ClientDealProposal.Proposal.VerifiedDeal,
		},
	}

	fm := metadata.Default.New(protocols...)

	// ensure we have a connection with the full node host so that the index provider gossip sub announcements make their
	// way to the filecoin bootstrapper network
	if err := w.meshCreator.Connect(ctx); err != nil {
		log.Errorw("failed to connect boost node to full daemon node", "err", err)
	}

	propCid, err := pds.SignedProposalCid()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get proposal cid from deal: %w", err)
	}

	annCid, err := w.prov.NotifyPut(ctx, nil, propCid.Bytes(), fm)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to announce deal to index provider: %w", err)
	}
	return annCid, err
}

func (w *Wrapper) DagstoreReinitBoostDeals(ctx context.Context) (bool, error) {
	deals, err := w.dealsDB.ListActive(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to list active Boost deals: %w", err)
	}

	log := log.Named("boost-migrator")
	log.Infof("dagstore root is %s", w.cfg.RootDir)

	// Check if all deals have already been registered as shards
	isComplete, err := w.boostRegistrationComplete()
	if err != nil {
		return false, fmt.Errorf("failed to get boost dagstore migration status: %w", err)
	}
	if isComplete {
		// All deals have been registered as shards, bail out
		log.Info("no boost shard migration necessary; already marked complete")
		return false, nil
	}

	log.Infow("registering shards for all active boost deals in sealing subsystem", "count", len(deals))

	// channel where results will be received, and channel where the total
	// number of registered shards will be sent.
	resch := make(chan dst.ShardResult, 32)
	totalCh := make(chan int)
	doneCh := make(chan struct{})

	// Start making progress consuming results. We won't know how many to
	// actually consume until we register all shards.
	//
	// If there are any problems registering shards, just log an error
	go func() {
		defer close(doneCh)

		var total = math.MaxInt64
		var res dst.ShardResult
		for rcvd := 0; rcvd < total; {
			select {
			case total = <-totalCh:
				// we now know the total number of registered shards
				// nullify so that we no longer consume from it after closed.
				close(totalCh)
				totalCh = nil
			case res = <-resch:
				rcvd++
				if res.Error == nil {
					log.Infow("async boost shard registration completed successfully", "shard_key", res.Key)
				} else {
					log.Warnw("async boost shard registration failed", "shard_key", res.Key, "error", res.Error)
				}
			}
		}
	}()

	var registered int
	for _, deal := range deals {
		pieceCid := deal.ClientDealProposal.Proposal.PieceCID

		// enrich log statements in this iteration with deal ID and piece CID.
		log := log.With("deal_id", deal.ChainDealID, "piece_cid", pieceCid)

		// Filter out deals that have not yet been indexed and announced as they will be re-indexed anyways
		if deal.Checkpoint < dealcheckpoints.IndexedAndAnnounced {
			continue
		}

		log.Infow("registering boost deal in dagstore with lazy init")

		// Register the deal as a shard with the DAG store with lazy initialization.
		// The index will be populated the first time the deal is retrieved, or
		// through the bulk initialization script.
		err = w.dagStore.RegisterShard(ctx, pieceCid, "", false, resch)
		if err != nil {
			log.Warnw("failed to register boost shard", "error", err)
			continue
		}
		registered++
	}

	log.Infow("finished registering all boost shards", "total", registered)
	totalCh <- registered
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-doneCh:
	}

	log.Infow("confirmed registration of all boost shards")

	// Completed registering all shards, so mark the migration as complete
	err = w.markBoostRegistrationComplete()
	if err != nil {
		log.Errorf("failed to mark boost shards as registered: %s", err)
	} else {
		log.Info("successfully marked boost migration as complete")
	}

	log.Infow("boost dagstore migration complete")

	return true, nil
}

// Check for the existence of a "marker" file indicating that the migration
// has completed
func (w *Wrapper) boostRegistrationComplete() (bool, error) {
	path := filepath.Join(w.cfg.RootDir, shardRegMarker)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Create a "marker" file indicating that the migration has completed
func (w *Wrapper) markBoostRegistrationComplete() error {
	path := filepath.Join(w.cfg.RootDir, shardRegMarker)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}
