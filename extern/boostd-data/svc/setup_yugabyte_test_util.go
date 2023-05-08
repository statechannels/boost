package svc

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	dockercl "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/filecoin-project/boostd-data/yugabyte"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"github.com/yugabyte/pgx/v4/pgxpool"
	"golang.org/x/net/context"
	"io"
	"os"
	"testing"
	"time"
)

var TestYugabyteSettings = yugabyte.DBSettings{
	Hosts:         []string{"127.0.0.1"},
	ConnectString: "postgresql://postgres:postgres@localhost",
}

func SetupYugabyte(t *testing.T) {
	ctx := context.Background()
	cli, err := dockercl.NewClientWithOpts(dockercl.FromEnv)
	require.NoError(t, err)

	imageName := "public.ecr.aws/n6b0k8i7/yugabyte-test:aarch64-2.17.2.0"
	out, err := cli.ImagePull(ctx, imageName, types.ImagePullOptions{})
	require.NoError(t, err)

	_, err = io.Copy(os.Stdout, out)
	require.NoError(t, err)

	tlog.Info("yugabyte docker container create...")
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			"7000": struct{}{},
			"9000": struct{}{},
			"5433": struct{}{},
			"9042": struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			"7000": {{HostIP: "127.0.0.1", HostPort: "7001"}},
			"9000": {{HostIP: "127.0.0.1", HostPort: "9000"}},
			// Yugabyte's postgres interface in docker runs on 5433
			// whereas the standard postgres port is 5432
			"5433": {{HostIP: "127.0.0.1", HostPort: "5432"}},
			"9042": {{HostIP: "127.0.0.1", HostPort: "9042"}},
		},
	}, nil, nil, "")
	require.NoError(t, err)
	tlog.Info("yugabyte docker container created")

	tlog.Info("yugabyte docker container start...")
	err = cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	require.NoError(t, err)
	tlog.Info("yugabyte docker container started")

	inspect, err := cli.ContainerInspect(ctx, resp.ID)
	require.NoError(t, err)
	spew.Dump(inspect)

	t.Cleanup(func() {
		tlog.Info("yugabyte docker container remove...")
		err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{Force: true})
		require.NoError(t, err)
		tlog.Info("yugabyte docker container removed")
	})

	tlog.Info("wait for yugabyte start...")
	awaitYugabyteUp(t, time.Minute)
	tlog.Info("yugabyte started")

	store := yugabyte.NewStore(TestYugabyteSettings)
	err = store.Start(ctx)
	require.NoError(t, err)

	RecreateTables(ctx, t, store)
}

func RecreateTables(ctx context.Context, t *testing.T, store *yugabyte.Store) {
	err := store.Drop(ctx)
	require.NoError(t, err)
	err = store.Create(ctx)
	require.NoError(t, err)
}

func awaitYugabyteUp(t *testing.T, duration time.Duration) {
	start := time.Now()
	cluster := gocql.NewCluster(TestYugabyteSettings.Hosts[0])
	for {
		_, err := cluster.CreateSession()
		if err == nil {
			_, err = pgxpool.Connect(context.Background(), TestYugabyteSettings.ConnectString)
			if err == nil {
				return
			}
		}

		tlog.Debugf("waiting for yugabyte: %s", err)
		if time.Since(start) > duration {
			t.Fatalf("failed to start yugabyte within %s", duration)
		}
		time.Sleep(time.Second)
	}
}
