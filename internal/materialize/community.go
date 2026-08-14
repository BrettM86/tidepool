package materialize

import (
	"context"
	"fmt"
	"strings"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// RecordGetter reads a committed record out of a repo. *repo.Manager
// implements it, as do the record seams ingest and votes already hold.
type RecordGetter interface {
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
}

// CommunityDIDOf answers which community's content a bridged record is. It is
// the membership question BOTH announced-delete authorization (ingest) and
// announced-vote binding (votes) ask, and it exists once because a fork
// between those two answers is a fork in who may moderate what.
//
// Before the author-owned flip the question answered itself: posts committed
// into the community's own repo, so the mapping's DID was the community's, and
// a comment borrowed its thread root's repo DID. After the flip a postv2 lives
// in the AUTHOR's repo and matches no community DID at all — so the answer is
// recorded on the mapping at materialization time (ap_objects.community_did,
// migration 016) and this function prefers it.
//
// The fallbacks are not decoration; they cover rows the column cannot:
//
//   - legacy posts, whose repo IS the community (backfilled by 016 too, but
//     stating it here keeps the two eras readable side by side);
//   - postv2 and comment rows written BEFORE 016, whose answer sits in the
//     record — a postv2's `community` field, a comment's thread root — where
//     no UPDATE statement could reach it.
//
// An empty return means "cannot be determined": no record, a malformed one, or
// a collection votes and deletes do not bind to. There are exactly TWO
// sanctioned readings of that, and no third:
//
//   - AUTHORIZATION callers (ingest's announced-delete check, votes'
//     announced-vote binding) must REFUSE — the safe direction is dropping a
//     legitimate announce, not admitting a foreign one.
//   - The MATERIALIZATION write path records it as unset, leaving the column
//     NULL so a later read can still derive the answer from the record.
//
// Nothing else may consume an empty value. In particular it must never be
// compared against a community DID, because "" == "" would make two unbindable
// subjects each other's community.
func CommunityDIDOf(ctx context.Context, records RecordGetter, mapping *store.APObjectMapping) (string, error) {
	switch mapping.Collection {
	case CollectionPost:
		// Deprecated era: the record is in the community's repo by definition,
		// so the repo DID is the answer whether or not the column was filled.
		return mapping.DID, nil
	case CollectionPostV2:
		if mapping.CommunityDID != "" {
			return mapping.CommunityDID, nil
		}
		record, err := readRecord(ctx, records, mapping.DID, mapping.Collection, mapping.RKey)
		if record == nil || err != nil {
			return "", err
		}
		community, _ := record["community"].(string)
		return community, nil
	case CollectionComment:
		if mapping.CommunityDID != "" {
			return mapping.CommunityDID, nil
		}
		record, err := readRecord(ctx, records, mapping.DID, mapping.Collection, mapping.RKey)
		if record == nil || err != nil {
			return "", err
		}
		return commentThreadCommunityDID(ctx, records, record)
	default:
		// Profiles and anything else: not community content.
		return "", nil
	}
}

// mappingCommunityDID is the WRITE side of CommunityDIDOf: which community a
// record being committed belongs to, for its mapping's community_did column.
// The two must agree, so each post era answers from the same thing the read
// side trusts for it — the repo for a legacy post, the record's own
// `community` for a postv2 — rather than from the caller's freshly-derived
// value, which on an update may name an audience the record itself rejected.
func mappingCommunityDID(collection, did string, record map[string]any, fallback, stored string) string {
	// A binding already made wins over anything this delivery derived. Which
	// community owns a piece of content is decided once, when it is first
	// materialized; every later delivery is just an edit of the content, and
	// an edit may not move content between communities' moderation authority.
	if stored != "" {
		return stored
	}
	switch collection {
	case CollectionPost:
		return did
	case CollectionPostV2:
		if community, ok := record["community"].(string); ok && community != "" {
			return community
		}
		return fallback
	default:
		// Comments carry no community field: the caller resolved it from the
		// thread. Profiles pass "" — they are not community content.
		return fallback
	}
}

// mappingThreadRootATURI is the thread a record being committed belongs to, for
// its mapping's thread_root_at_uri column.
//
// Only a COMMENT has one to record. A post IS the top of its thread, and every
// reader already treats it that way, so writing its own at-uri back at it would
// add a second spelling of a fact the collection alone already answers.
//
// The value comes from the record's own reply.root — the strongRef the
// materializer just resolved through the thread (resolveReplyRefs), never from
// anything a delivery asserted about itself.
func mappingThreadRootATURI(collection string, record map[string]any, stored string) string {
	if stored != "" {
		return stored
	}
	if collection != CollectionComment {
		return ""
	}
	did, rootCollection, rkey := replyRootRef(record)
	if did == "" || rootCollection == "" || rkey == "" {
		return ""
	}
	return "at://" + did + "/" + rootCollection + "/" + rkey
}

// commentThreadCommunityDID recovers a comment's community from its thread
// root. Which era the root belongs to decides how: a legacy root's repo IS the
// community, a postv2 root only NAMES one, so the root's own record has to be
// read for it. The materializer guarantees every comment carries reply.root,
// so this is one hop and never a walk.
func commentThreadCommunityDID(ctx context.Context, records RecordGetter, comment map[string]any) (string, error) {
	rootDID, rootCollection, rootRKey := replyRootRef(comment)
	if rootDID == "" {
		return "", nil
	}
	if rootCollection != CollectionPostV2 {
		return rootDID, nil
	}
	root, err := readRecord(ctx, records, rootDID, rootCollection, rootRKey)
	if root == nil || err != nil {
		return "", err
	}
	community, _ := root["community"].(string)
	return community, nil
}

// readRecord fetches a record, mapping "not found" to (nil, nil): a live
// mapping whose record is gone cannot authorize anything, and that is an
// answer — not a retryable failure that would wedge an ordering key behind it.
func readRecord(ctx context.Context, records RecordGetter, did, collection, rkey string) (map[string]any, error) {
	record, _, err := records.GetRecord(ctx, did, collection, rkey)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("materialize: read %s/%s/%s for community binding: %w", did, collection, rkey, err)
	}
	return record, nil
}

// replyRootRef splits a comment record's reply.root strongRef uri
// (at://did/collection/rkey) into its parts. Anything malformed yields empty
// strings, which callers read as "cannot be determined".
func replyRootRef(record map[string]any) (did, collection, rkey string) {
	reply, ok := record["reply"].(map[string]any)
	if !ok {
		return "", "", ""
	}
	root, ok := reply["root"].(map[string]any)
	if !ok {
		return "", "", ""
	}
	uri, _ := root["uri"].(string)
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return "", "", ""
	}
	did, rest, _ = strings.Cut(rest, "/")
	collection, rkey, _ = strings.Cut(rest, "/")
	return did, collection, rkey
}
