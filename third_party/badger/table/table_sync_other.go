//go:build !linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-10-26. See hoardDB/third_party/README.md.
 */

package table

import "github.com/dgraph-io/ristretto/v2/z"

// platformTableSync has no ranged writeback off Linux, so syncNewTable uses
// the one msync whatever the mode.
var platformTableSync = tableSyncCalls{msync: z.Msync}
