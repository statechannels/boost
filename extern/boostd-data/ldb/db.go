package ldb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	levelds "github.com/ipfs/go-ds-leveldb"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/syndtr/goleveldb/leveldb/opt"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	// LevelDB key value for storing next free cursor.
	keyNextCursor   uint64 = 0
	dskeyNextCursor datastore.Key

	// LevelDB key prefix for PieceCid to cursor table.
	// LevelDB keys will be built by concatenating PieceCid to this prefix.
	prefixPieceCidToCursor  uint64 = 1
	sprefixPieceCidToCursor string

	// LevelDB key prefix for Multihash to PieceCids table.
	// LevelDB keys will be built by concatenating Multihash to this prefix.
	prefixMhtoPieceCids  uint64 = 2
	sprefixMhtoPieceCids string

	// LevelDB key prefix for Flagged pieces table.
	// LevelDB keys will be built by concatenating PieceCid to this prefix.
	prefixPieceCidToFlagged  uint64 = 3
	sprefixPieceCidToFlagged string

	/////////////////////////////////////////
	// Prefixes up to 100 are system prefixes
)

func init() {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, keyNextCursor)
	dskeyNextCursor = datastore.NewKey(string(buf))

	buf = make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, prefixPieceCidToCursor)
	sprefixPieceCidToCursor = string(buf)

	buf = make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, prefixMhtoPieceCids)
	sprefixMhtoPieceCids = string(buf)

	buf = make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, prefixPieceCidToFlagged)
	sprefixPieceCidToFlagged = string(buf)
}

type DB struct {
	datastore.Batching
}

func newDB(path string, readonly bool) (*DB, error) {
	ldb, err := levelds.NewDatastore(path, &levelds.Options{
		Compression:         ldbopts.SnappyCompression,
		NoSync:              true,
		Strict:              ldbopts.StrictAll,
		ReadOnly:            readonly,
		CompactionTableSize: 4 * opt.MiB,
	})
	if err != nil {
		return nil, fmt.Errorf("creating level db datstore: %w", err)
	}

	return &DB{ldb}, nil
}

func (db *DB) InitCursor(ctx context.Context) error {
	_, err := db.Get(ctx, dskeyNextCursor)
	if err == nil {
		// Cursor has already been initialized, so just return
		log.Debug("leveldb cursor already initialized")
		return nil
	}

	if errors.Is(err, ds.ErrNotFound) {
		// Cursor has not yet been initialized so initialize it
		log.Debug("initializing leveldb cursor")
		err = db.SetNextCursor(ctx, 100)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("initializing database cursor: %w", err)
}

// NextCursor
func (db *DB) NextCursor(ctx context.Context) (uint64, string, error) {
	b, err := db.Get(ctx, dskeyNextCursor)
	if err != nil {
		return 0, "", err
	}

	cursor, _ := binary.Uvarint(b)
	return cursor, fmt.Sprintf("%d", cursor) + "/", nil // adding "/" because of Query method in go-datastore
}

// SetNextCursor
func (db *DB) SetNextCursor(ctx context.Context, cursor uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, cursor)

	return db.Put(ctx, dskeyNextCursor, buf)
}

// GetPieceCidsByMultihash
func (db *DB) GetPieceCidsByMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cids_by_multihash")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixMhtoPieceCids, mh.String()))

	val, err := db.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for multihash %s, err: %w", mh, err)
	}

	var pcids []cid.Cid
	if err := json.Unmarshal(val, &pcids); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pieceCids slice: %w", err)
	}

	return pcids, nil
}

// SetMultihashToPieceCid
func (db *DB) SetMultihashesToPieceCid(ctx context.Context, recs []carindex.Record, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_multihashes_to_piece_cid")
	defer span.End()

	batch, err := db.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

	for _, r := range recs {
		mh := r.Cid.Hash()

		err := func() error {
			key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixMhtoPieceCids, mh.String()))

			// do we already have an entry for this multihash ?
			val, err := db.Get(ctx, key)
			if err != nil && err != ds.ErrNotFound {
				return fmt.Errorf("failed to get value for multihash %s, err: %w", mh, err)
			}

			// if we don't have an existing entry for this mh, create one
			if err == ds.ErrNotFound {
				v := []cid.Cid{pieceCid}
				b, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("failed to marshal pieceCids slice: %w", err)
				}

				if err := batch.Put(ctx, key, b); err != nil {
					return fmt.Errorf("failed to batch put mh=%s, err=%w", mh, err)
				}
				return nil
			}

			// else, append the pieceCid to the existing list
			var pcids []cid.Cid
			if err := json.Unmarshal(val, &pcids); err != nil {
				return fmt.Errorf("failed to unmarshal pieceCids slice: %w", err)
			}

			// if we already have the pieceCid indexed for the multihash, nothing to do here.
			if has(pcids, pieceCid) {
				return nil
			}

			pcids = append(pcids, pieceCid)

			b, err := json.Marshal(pcids)
			if err != nil {
				return fmt.Errorf("failed to marshal pieceCids slice: %w", err)
			}
			if err := batch.Put(ctx, key, b); err != nil {
				return fmt.Errorf("failed to batch put mh=%s, err%w", mh, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	key := datastore.NewKey(sprefixMhtoPieceCids)
	if err := db.Sync(ctx, key); err != nil {
		return fmt.Errorf("failed to sync puts: %w", err)
	}

	return nil
}

// SetPieceCidToFlagged
func (db *DB) SetPieceCidToFlagged(ctx context.Context, pieceCid cid.Cid, fm LeveldbFlaggedMetadata) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_piece_cid_to_flagged")
	defer span.End()

	b, err := json.Marshal(fm)
	if err != nil {
		return err
	}

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToFlagged, pieceCid.String()))

	return db.Put(ctx, key, b)
}

// GetPieceCidToFlagged
func (db *DB) GetPieceCidToFlagged(ctx context.Context, pieceCid cid.Cid) (LeveldbFlaggedMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cid_to_flagged")
	defer span.End()

	var metadata LeveldbFlaggedMetadata

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToFlagged, pieceCid.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return metadata, fmt.Errorf("getting flagged metadata for piece %s: %w", pieceCid, err)
	}

	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return metadata, fmt.Errorf("unmarshaling flagged metadata for piece %s: %w", pieceCid, err)
	}

	return metadata, nil
}

// SetPieceCidToMetadata
func (db *DB) SetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid, md LeveldbMetadata) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_piece_cid_to_metadata")
	defer span.End()

	b, err := json.Marshal(md)
	if err != nil {
		return err
	}

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToCursor, pieceCid.String()))

	return db.Put(ctx, key, b)
}

// GetPieceCidToMetadata
func (db *DB) GetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid) (LeveldbMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cid_to_metadata")
	defer span.End()

	var metadata LeveldbMetadata

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToCursor, pieceCid.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return metadata, fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return metadata, fmt.Errorf("unmarshaling piece metadata for piece %s: %w", pieceCid, err)
	}

	return metadata, nil
}

func (db *DB) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, sourceErr error) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.mark_piece_index_errored")
	defer span.End()

	md, err := db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	if md.Error != "" {
		// If the error state has already been set, don't over-write the existing error
		return nil
	}

	md.Error = sourceErr.Error()
	md.ErrorType = fmt.Sprintf("%T", sourceErr)

	return db.SetPieceCidToMetadata(ctx, pieceCid, md)
}

// AllRecords
func (db *DB) AllRecords(ctx context.Context, cursor uint64) ([]model.Record, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.all_records")
	defer span.End()

	var records []model.Record

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, cursor)

	var q query.Query
	q.Prefix = fmt.Sprintf("%d/", cursor)
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[len(q.Prefix)+1:]

		m, err := multihash.FromHexString(k)
		if err != nil {
			return nil, err
		}

		kcid := cid.NewCidV1(cid.Raw, m)

		offset, n := binary.Uvarint(r.Value)
		size, _ := binary.Uvarint(r.Value[n:])

		records = append(records, model.Record{
			Cid: kcid,
			OffsetSize: model.OffsetSize{
				Offset: offset,
				Size:   size,
			},
		})
	}

	return records, nil
}

// AddIndexRecord
func (db *DB) AddIndexRecord(ctx context.Context, cursorPrefix string, rec model.Record) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.add_index_record")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, rec.Cid.Hash().String()))

	value := make([]byte, 2*binary.MaxVarintLen64)
	no := binary.PutUvarint(value, rec.Offset)
	ns := binary.PutUvarint(value[no:], rec.Size)

	return db.Put(ctx, key, value[:no+ns])
}

// GetOffsetSize
func (db *DB) GetOffsetSize(ctx context.Context, cursorPrefix string, m multihash.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_offset")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	offset, n := binary.Uvarint(b)
	size, _ := binary.Uvarint(b[n:])
	return &model.OffsetSize{
		Offset: offset,
		Size:   size,
	}, nil
}

var (
	// The minimum frequency with which to check pieces for errors (eg bad index)
	MinPieceCheckPeriod = 30 * time.Second

	// in-memory cursor to the position we reached in the leveldb table with respect to piece cids to process for errors with the doctor
	offset int

	// checked keeps track in memory when was the last time we processed a given piece cid
	checked map[string]time.Time

	// batch limit for each NextPiecesToCheck call
	PiecesToTrackerBatchSize = 1024
)

func init() {
	checked = make(map[string]time.Time)
}

func (db *DB) NextPiecesToCheck(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.next_pieces_to_check")
	defer span.End()

	q := query.Query{
		Prefix:   "/" + sprefixPieceCidToCursor + "/",
		KeysOnly: true,
		Limit:    PiecesToTrackerBatchSize,
		Offset:   offset,
	}
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("listing pieces in database: %w", err)
	}

	var pieceCids []cid.Cid

	now := time.Now()

	var i int
	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}
		i++

		k := r.Key[len(q.Prefix):]
		if t, ok := checked[k]; ok {
			alreadyChecked := t.After(now.Add(-MinPieceCheckPeriod))

			if alreadyChecked {
				continue
			}
		}
		checked[k] = now

		pieceCid, err := cid.Parse(k)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid '%s': %w", k, err)
		}

		pieceCids = append(pieceCids, pieceCid)
	}
	offset += i

	// if we got less pieces than the specified limit, we must be at the end of the table,
	// so reset the cursor
	if i < PiecesToTrackerBatchSize-1 {
		offset = 0
	}

	log.Debugw("NextPiecesToCheck: returning piececids", "len", len(pieceCids), "offset", offset)

	return pieceCids, nil
}

func (db *DB) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.list_pieces")
	defer span.End()

	q := query.Query{
		Prefix:   "/" + sprefixPieceCidToCursor + "/",
		KeysOnly: true,
	}
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("listing pieces in database: %w", err)
	}

	var pieceCids []cid.Cid
	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[len(q.Prefix):]
		pieceCid, err := cid.Parse(k)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid '%s': %w", k, err)
		}

		pieceCids = append(pieceCids, pieceCid)
	}

	return pieceCids, nil
}

func has(list []cid.Cid, v cid.Cid) bool {
	for _, l := range list {
		if l.Equals(v) {
			return true
		}
	}
	return false
}

// RemoveMetadata
func (db *DB) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.remove_piece_metadata")
	defer span.End()

	var metadata LeveldbMetadata

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToCursor, pieceCid.String()))

	piece, err := db.Get(ctx, key)
	if err != nil {
		return err
	}

	err = json.Unmarshal(piece, &metadata)
	if err != nil {
		return fmt.Errorf("error while reading metadata: %w", err)
	}

	// Remove all multihashes before, as without Metadata, they are useless
	// This order is important as metadata.Cursor is required in case RemoveAllRecords fails
	// and needs to be run manually
	if err = db.RemoveIndexes(ctx, metadata.Cursor, pieceCid); err != nil {
		return err
	}

	// TODO: Requires DB compaction for removing the key
	if err = db.Delete(ctx, key); err != nil {
		return err
	}

	return nil
}

// RemoveIndexes
// It removes multihash -> pieceCid and if empty record is left then multihash -> offset
// entry is also removed
func (db *DB) RemoveIndexes(ctx context.Context, cursor uint64, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.remove_indexes")
	defer span.End()

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, cursor)

	var q query.Query
	q.Prefix = fmt.Sprintf("%d/", cursor)
	results, err := db.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("error querying the database:  %w", err)
	}

	batch, err := db.Batch(ctx)
	if err != nil {
		return fmt.Errorf("error in creating batching:  %w", err)
	}

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		m := r.Key[len(q.Prefix)+1:]

		err = func() error {
			key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixMhtoPieceCids, m))

			val, err := db.Get(ctx, key)
			if err != nil && err != ds.ErrNotFound {
				return fmt.Errorf("failed to get value for multihash %s, err: %w", m, err)
			}

			if errors.Is(err, ds.ErrNotFound) {
				return nil
			}

			var pcids []cid.Cid
			if err := json.Unmarshal(val, &pcids); err != nil {
				return fmt.Errorf("failed to unmarshal pieceCids slice: %w", err)
			}

			if !has(pcids, pieceCid) {
				return nil
			}

			if len(pcids) <= 1 {
				// Remove multihash -> pieceCId (key+value)
				if err := batch.Delete(ctx, key); err != nil {
					return fmt.Errorf("failed to batch delete multihash to pieceCid mh=%s, pieceCid=%s err%w", key, pcids[0], err)
				}
				return nil
			}

			// Remove multihash -> pieceCId (value only)
			for i, v := range pcids {
				if v == pieceCid {
					pcids[i] = pcids[len(pcids)-1]
					pcids = pcids[:len(pcids)-1]
				}
			}

			b, err := json.Marshal(pcids)
			if err != nil {
				return fmt.Errorf("failed to marshal pieceCids slice: %w", err)
			}
			if err := batch.Put(ctx, key, b); err != nil {
				return fmt.Errorf("failed to batch put mh=%s, err%w", m, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}

		// Remove (cursor+multihash) -> Offset
		if err := batch.Delete(ctx, ds.NewKey(r.Key)); err != nil {
			return fmt.Errorf("failed to batch delete mh=%s, err%w", r.Key, err)
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

func (db *DB) ListFlaggedPieces(ctx context.Context) ([]model.FlaggedPiece, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.list_flagged_pieces")
	defer span.End()

	q := query.Query{
		Prefix:   "/" + sprefixPieceCidToFlagged + "/",
		KeysOnly: false,
	}
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("listing flagged pieces in database: %w", err)
	}

	var records []model.FlaggedPiece
	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[len(q.Prefix):]
		pieceCid, err := cid.Parse(k)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid '%s': %w", k, err)
		}

		var v LeveldbFlaggedMetadata
		err = json.Unmarshal(r.Value, &v)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal LeveldbFlaggedMetadata: %w; %v", err, r.Value)
		}

		records = append(records, model.FlaggedPiece{CreatedAt: v.CreatedAt, PieceCid: pieceCid})
	}

	return records, nil
}

func (db *DB) FlaggedPiecesCount(ctx context.Context) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.flagged_pieces_count")
	defer span.End()

	q := query.Query{
		Prefix:   "/" + sprefixPieceCidToFlagged + "/",
		KeysOnly: true,
	}
	results, err := db.Query(ctx, q)
	if err != nil {
		return -1, fmt.Errorf("listing flagged pieces in database: %w", err)
	}

	var i int
	for {
		_, ok := results.NextSync()
		if !ok {
			break
		}

		i++
	}

	return i, nil
}

// DeletePieceCidToFlagged
func (db *DB) DeletePieceCidToFlagged(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.delete_piece_flagged_metadata")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToFlagged, pieceCid.String()))

	// TODO: Requires DB compaction for removing the key
	return db.Delete(ctx, key)
}
