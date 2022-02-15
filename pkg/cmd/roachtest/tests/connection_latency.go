// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

const (
	regionUsEast    = "us-east1-b"
	regionUsCentral = "us-central1-b"
	regionUsWest    = "us-west1-b"
	regionEuWest    = "europe-west2-b"
)

func runConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int, numZones int, password bool,
) {
	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach")
	require.NoError(t, err)

	err = c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	err = c.StartE(ctx, option.StartArgs("--secure"))
	require.NoError(t, err)

	var passwordFlag string
	// Only create the user once.
	t.L().Printf("creating testuser")
	if password {
		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser WITH PASSWORD '123' CREATEDB"`)
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --password '123' --secure")
		require.NoError(t, err)
		passwordFlag = "--password 123 "
	} else {
		// NB: certs for `testuser` are created by `roachprod start --secure`.
		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser CREATEDB"`)
		require.NoError(t, err)
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --secure")
		require.NoError(t, err)
	}

	runWorkload := func(roachNodes, loadNode option.NodeListOption, locality string) {
		var urlString string
		var urls []string
		externalIps, err := c.ExternalIP(ctx, roachNodes)
		require.NoError(t, err)

		if password {
			urlTemplate := "postgres://testuser:123@%s:26257?sslmode=require&sslrootcert=certs/ca.crt"
			for _, u := range externalIps {
				url := fmt.Sprintf(urlTemplate, u)
				urls = append(urls, fmt.Sprintf("'%s'", url))
			}
			urlString = strings.Join(urls, " ")
		} else {
			urlTemplate := "postgres://testuser@%s:26257?sslcert=certs/client.testuser.crt&sslkey=certs/client.testuser.key&sslrootcert=certs/ca.crt&sslmode=require"
			for _, u := range externalIps {
				url := fmt.Sprintf(urlTemplate, u)
				urls = append(urls, fmt.Sprintf("'%s'", url))
			}
			urlString = strings.Join(urls, " ")
		}

		t.L().Printf("running workload in %q against urls:\n%s", locality, strings.Join(urls, "\n"))

		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --user testuser --secure %s --duration 30s --histograms=%s/stats.json --prometheus-port=%d --locality %s`,
			urlString,
			passwordFlag,
			t.PerfArtifactsDir(),
			2112,
			locality,
		)
		err = c.RunE(ctx, loadNode, workloadCmd)
		require.NoError(t, err)
	}

	if numZones > 1 {
		numLoadNodes := numZones
		loadGroups := makeLoadGroups(c, numZones, numNodes, numLoadNodes)
		cockroachUsEast := loadGroups[0].loadNodes
		cockroachUsWest := loadGroups[1].loadNodes
		cockroachEuWest := loadGroups[2].loadNodes

		_, cleanup := setupPrometheusConnectionLatency(ctx, t, c, loadGroups.loadNodes())
		defer cleanup()

		runWorkload(loadGroups[0].roachNodes, cockroachUsEast, regionUsEast)
		runWorkload(loadGroups[1].roachNodes, cockroachUsWest, regionUsWest)
		runWorkload(loadGroups[2].roachNodes, cockroachEuWest, regionEuWest)
	} else {
		_, cleanup := setupPrometheusConnectionLatency(ctx, t, c, c.Node(c.Spec().NodeCount))
		defer cleanup()
		// Run only on the load node.
		runWorkload(c.Range(1, numNodes), c.Node(numNodes+1), regionUsCentral)
	}
}

// setupPrometheusConnectionLatency initializes prometheus to run against a new
// PrometheusConfig. It creates a prometheus scraper for all load nodes.
// Returns the created PrometheusConfig if prometheus is initialized, as well
// as a cleanup function which should be called in a defer statement.
func setupPrometheusConnectionLatency(
	ctx context.Context, t test.Test, c cluster.Cluster, loadNodes option.NodeListOption,
) (*prometheus.Config, func()) {
	// Avoid setting prometheus automatically up for local clusters.
	if c.IsLocal() {
		return nil, func() {}
	}
	workloadScrapeNodes := []prometheus.ScrapeNode{
		{
			Nodes: loadNodes,
			Port:  2112,
		},
	}
	cfg := &prometheus.Config{
		PrometheusNode: loadNodes,
		ScrapeConfigs: []prometheus.ScrapeConfig{
			prometheus.MakeWorkloadScrapeConfig("workload", workloadScrapeNodes),
		},
	}
	p, err := prometheus.Init(
		ctx,
		*cfg,
		c,
		func(ctx context.Context, nodes option.NodeListOption, operation string, args ...string) error {
			return repeatRunE(
				ctx,
				t,
				c,
				nodes,
				operation,
				args...,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	return cfg, func() {
		// Use a context that will not time out to avoid the issue where
		// ctx gets canceled if t.Fatal gets called.
		snapshotCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Snapshot(
			snapshotCtx,
			c,
			t.L(),
			filepath.Join(t.ArtifactsDir(), "prometheus-snapshot.tar.gz"),
		); err != nil {
			t.L().Printf("failed to get prometheus snapshot: %v", err)
		}
	}
}

func registerConnectionLatencyTest(r registry.Registry) {
	// Single region test.
	numNodes := 3
	r.Add(registry.TestSpec{
		Name:  fmt.Sprintf("connection_latency/nodes=%d/certs", numNodes),
		Owner: registry.OwnerSQLExperience,
		// Add one more node for load node.
		Cluster: r.MakeClusterSpec(numNodes+1, spec.Zones(regionUsCentral)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numNodes, 1, false /*password*/)
		},
	})

	geoZones := []string{regionUsEast, regionUsWest, regionEuWest}
	geoZonesStr := strings.Join(geoZones, ",")
	numMultiRegionNodes := 9
	numZones := len(geoZones)
	loadNodes := numZones

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/multiregion/certs", numMultiRegionNodes),
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, false /*password*/)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/multiregion/password", numMultiRegionNodes),
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, true /*password*/)
		},
	})
}
