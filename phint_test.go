package main

import (
	"github.com/MasahikoSawada/phint/pgplan"
	"testing"
)

func TestBitmapOR(t *testing.T) {
	sqlJson := `
[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Parallel Aware": false,
      "Join Type": "Inner",
      "Inner Unique": true,
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Relation Name": "tbl2",
          "Alias": "tbl2"
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Plans": [
            {
              "Node Type": "Bitmap Heap Scan",
              "Parent Relationship": "Outer",
              "Parallel Aware": false,
              "Relation Name": "tbl1",
              "Alias": "tbl1",
              "Plans": [
                {
                  "Node Type": "BitmapOr",
                  "Parent Relationship": "Outer",
                  "Parallel Aware": false,
                  "Plans": [
                    {
                      "Node Type": "Bitmap Index Scan",
                      "Parent Relationship": "Member",
                      "Parallel Aware": false,
                      "Index Name": "tbl1_pkey"
                    },
                    {
                      "Node Type": "Bitmap Index Scan",
                      "Parent Relationship": "Member",
                      "Parallel Aware": false,
                      "Index Name": "tbl1_pkey"
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
`
	plan := pgplan.GetPlanFromJson(sqlJson)
	hint := pgplan.GetHintFromPlan(plan)
	res := hint.GetAllHints()
	expected := `/*+
Leading((tbl2 tbl1))
HashJoin(tbl2 tbl1)
SeqScan(tbl2)
BitmapScan(tbl1 tbl1_pkey)
*/`
	if res != expected {
		t.Fatalf("failed BitmapOr test, got = %s, expected = %s", res, expected)
	}
}

func TestHash(t *testing.T) {
	sqlJson := `
[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Parallel Aware": false,
      "Join Type": "Inner",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Relation Name": "tbl2",
          "Alias": "tbl2"
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Plans": [
            {
              "Node Type": "Seq Scan",
              "Parent Relationship": "Outer",
              "Parallel Aware": false,
              "Relation Name": "tbl1",
              "Alias": "tbl1"
            }
          ]
        }
      ]
    }
  }
]
`
	plan := pgplan.GetPlanFromJson(sqlJson)
	hint := pgplan.GetHintFromPlan(plan)
	res := hint.GetAllHints()
	expected := `/*+
Leading((tbl2 tbl1))
HashJoin(tbl2 tbl1)
SeqScan(tbl2)
SeqScan(tbl1)
*/`
	if res != expected {
		t.Fatalf("failed BitmapOr test, got = %s, expected = %s", res, expected)
	}

}

func TestCte(t *testing.T) {
	sqlJson := `
[
  {
    "Plan": {
      "Node Type": "Nested Loop",
      "Join Type": "Inner",
      "Plans": [
        {
          "Node Type": "Index Only Scan",
          "Parent Relationship": "InitPlan",
          "Subplan Name": "CTE aaa",
          "Index Name": "hoge_idx",
          "Relation Name": "hoge",
          "Alias": "hoge"
        },
        {
          "Node Type": "Nested Loop",
          "Parent Relationship": "Outer",
          "Join Type": "Inner",
          "Plans": [
            {
              "Node Type": "CTE Scan",
              "Parent Relationship": "Outer",
              "CTE Name": "aaa",
              "Alias": "aaa"
            },
            {
              "Node Type": "Index Only Scan",
              "Parent Relationship": "Inner",
              "Index Name": "tbl2_pkey",
              "Relation Name": "tbl2",
              "Alias": "tbl2"
            }
          ]
        },
        {
          "Node Type": "Index Only Scan",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Index Name": "tbl1_pkey",
          "Relation Name": "tbl1",
          "Alias": "tbl1"
        }
      ]
    }
  }
]
`
	plan := pgplan.GetPlanFromJson(sqlJson)
	hint := pgplan.GetHintFromPlan(plan)
	res := hint.GetAllHints()
	expected := `/*+
Leading((hoge (aaa tbl2) tbl1))
NestLoop(hoge aaa tbl2 tbl1)
NestLoop(aaa tbl2)
IndexOnlyScan(hoge)
IndexOnlyScan(tbl2)
IndexOnlyScan(tbl1)
*/`
	if res != expected {
		t.Fatalf("failed BitmapOr test, got = %s, expected = %s", res, expected)
	}
}
