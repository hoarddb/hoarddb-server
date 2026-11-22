package main

// The coverage declaration (the domain-feature coverage plan, stage S0): every record says which
// hoardDB abilities its leg exercised, so a figure is never read as covering more than it did. A
// capability is a product feature of the entity-centric design, not a layout choice. The list is
// empty for an engine that has no such features, and is always written, so "none" and "not
// declared" can be told apart. Abilities the product has but no leg uses yet (declared sums are
// read by no request, no inc, no expiry, no nearest-k, no memory database) are listed in the README.
const (
	capEntityBuckets      = "entity-buckets"
	capStoreFields        = "store-fields"
	capClaimAck           = "claim-ack"
	capBatchFrame         = "batch-frame"
	capGetMany            = "get-many"
	capNativeLayout       = "native-layout"
	capDeclaredAggregates = "declared-aggregates"
)

// capabilityDescriptions is the vocabulary: a record may carry only these names.
var capabilityDescriptions = map[string]string{
	capEntityBuckets:      "every entity is its own small bucket in one shared store",
	capStoreFields:        "the entity's collections are store fields (fifo, hash, btree) of its document",
	capClaimAck:           "agent tasks are taken with claim and ack leases",
	capBatchFrame:         "the several writes of one operation go as one batch frame",
	capGetMany:            "the documents a page names are read with get-many frames",
	capNativeLayout:       "feed and member elements carry copies of what a page needs",
	capDeclaredAggregates: "the ledger declares maintained sums and counts that every write keeps",
}

// capabilitiesOf lists what the record's leg exercised, in a fixed order, judged by what the run
// did and not by its flags alone: a switch that nothing used is not listed. A leg of another engine
// exercises none of these hoardDB abilities.
func capabilitiesOf(r *Record) []string {
	list := []string{}
	if r.Engine != "ring" || r.Reads+r.Writes == 0 {
		return list
	}
	list = append(list, capEntityBuckets, capStoreFields)
	if r.Ops[string(WriteAgentTask)].Count > 0 {
		list = append(list, capClaimAck)
	}
	if r.Batch == "on" && r.Writes > 0 {
		list = append(list, capBatchFrame)
	}
	pages := r.Ops[string(ReadSocialFeed)].Count + r.Ops[string(ReadChatRoomPage)].Count
	if r.MultiGet == "on" && r.Native != "on" && pages > 0 {
		list = append(list, capGetMany)
	}
	if r.Native == "on" && pages > 0 {
		list = append(list, capNativeLayout)
	}
	if r.Aggregates == "on" && r.MaintainedSumsAudited > 0 {
		list = append(list, capDeclaredAggregates)
	}
	return list
}
