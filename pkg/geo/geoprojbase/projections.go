// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file was generated from `./pkg/cmd/generate-spatial-ref-sys`.

package geoprojbase

import (
	"bytes"
	_ "embed" // required for go:embed
	"sync"

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase/embeddedproj"
	"github.com/cockroachdb/errors"
)

//go:embed data/proj.json.gz
var projData []byte

var once sync.Once
var projectionsInternal map[geopb.SRID]ProjInfo

// getProjections returns the mapping of SRID to projections.
// Use the `Projection` function to obtain one.
func getProjections() map[geopb.SRID]ProjInfo {
	once.Do(func() {
		d, err := embeddedproj.Decode(bytes.NewReader(projData))
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "error decoding embedded projection data"))
		}

		// Build a temporary map of spheroids so we can look them up by hash.
		spheroids := make(map[int64]*geographiclib.Spheroid, len(d.Spheroids))
		for _, s := range d.Spheroids {
			spheroids[s.Hash] = geographiclib.NewSpheroid(s.Radius, s.Flattening)
		}

		projectionsInternal = make(map[geopb.SRID]ProjInfo, len(d.Projections))
		for _, p := range d.Projections {
			srid := geopb.SRID(p.SRID)
			spheroid, ok := spheroids[p.Spheroid]
			if !ok {
				panic(errors.AssertionFailedf("embedded projection data contains invalid spheroid %x", p.Spheroid))
			}
			projectionsInternal[srid] = ProjInfo{
				SRID:      srid,
				AuthName:  "EPSG",
				AuthSRID:  p.AuthSRID,
				SRText:    p.SRText,
				Proj4Text: MakeProj4Text(p.Proj4Text),
				Bounds: Bounds{
					MinX: p.Bounds.MinX,
					MaxX: p.Bounds.MaxX,
					MinY: p.Bounds.MinY,
					MaxY: p.Bounds.MaxY,
				},
				IsLatLng: p.IsLatLng,
				Spheroid: spheroid,
			}
		}
	})

	return projectionsInternal
}
