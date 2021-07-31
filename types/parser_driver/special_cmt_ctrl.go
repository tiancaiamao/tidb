// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"fmt"
	"regexp"
)

// SpecialCommentVersionPrefix is the prefix of TiDB executable comments.
const SpecialCommentVersionPrefix = `/*T!`

// BuildSpecialCommentPrefix returns the prefix of `featureID` special comment.
// For some special feature in TiDB, we will refine ddl query with special comment,
// which may be useful when
// A: the downstream is directly MySQL instance (treat it as comment for compatibility).
// B: the downstream is lower version TiDB (ignore the unknown feature comment).
// C: the downstream is same/higher version TiDB (parse the feature syntax out).
func BuildSpecialCommentPrefix(featureID featureID) string {
	return fmt.Sprintf("%s[%s]", SpecialCommentVersionPrefix, featureID)
}

type featureID string

const (
	// FeatureIDAutoRandom is the `auto_random` feature.
	FeatureIDAutoRandom featureID = "auto_rand"
	// FeatureIDAutoIDCache is the `auto_id_cache` feature.
	FeatureIDAutoIDCache featureID = "auto_id_cache"
	// FeatureIDAutoRandomBase is the `auto_random_base` feature.
	FeatureIDAutoRandomBase featureID = "auto_rand_base"
	// FeatureClusteredIndex is the `clustered_index` feature.
	FeatureClusteredIndex featureID = "clustered_index"
	// FeatureForceAutoInc is the `force auto_increment` feature.
	FeatureForceAutoInc featureID = "force_inc"
)

// FeatureIDPatterns is used to record special comments patterns.
var FeatureIDPatterns = map[featureID]*regexp.Regexp{
	FeatureIDAutoRandom:     regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_RANDOM\b\s*(\s*\(\s*\d+\s*\)\s*)?)`),
	FeatureIDAutoIDCache:    regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_ID_CACHE\s*=?\s*\d+\s*)`),
	FeatureIDAutoRandomBase: regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_RANDOM_BASE\s*=?\s*\d+\s*)`),
	FeatureClusteredIndex:   regexp.MustCompile(`(?i)(PRIMARY)?\s+KEY(\s*\(.*\))?\s+(?P<REPLACE>(NON)?CLUSTERED\b)`),
	FeatureForceAutoInc:     regexp.MustCompile(`(?P<REPLACE>(?i)FORCE)\b\s*AUTO_INCREMENT\s*`),
}
