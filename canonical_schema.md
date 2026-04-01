# **Data Contract Enforcer - Schema Integrity & Lineage Attribution System**

## **Week 1 — Intent-Code Correlator  (intent\_record)**

// File: outputs/week1/intent\_records.jsonl  
{  
  "intent\_id":    "uuid-v4",  
  "description":  "string — plain-English statement of intent",  
  "code\_refs": \[  
    {  
      "file":       "relative/path/from/repo/root.py",  
      "line\_start": 42,           // int, 1-indexed  
      "line\_end":   67,           // int \>= line\_start  
      "symbol":     "function\_or\_class\_name",  
      "confidence": 0.87         // float MUST be 0.0–1.0  
    }  
  \],  
  "governance\_tags": \["auth", "pii", "billing"\],  
  "created\_at":      "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** confidence is float 0.0–1.0; created\_at is ISO 8601; code\_refs\[\] is non-empty; every file path exists in the repo.

## **Week 2 — Digital Courtroom  (verdict\_record)**

// File: outputs/week2/verdicts.jsonl  
{  
  "verdict\_id":      "uuid-v4",  
  "target\_ref":      "relative/path/or/doc\_id",  
  "rubric\_id":       "sha256\_hash\_of\_rubric\_yaml",  
  "rubric\_version":  "1.2.0",  // semver  
  "scores": {  
    "criterion\_name": {  
      "score":    3,            // int MUST be 1–5  
      "evidence": \["string excerpt..."\],  
      "notes":    "string"  
    }  
  },  
  "overall\_verdict":  "PASS",  // enum: PASS | FAIL | WARN  
  "overall\_score":    3.4,      // float, weighted average of scores  
  "confidence":       0.91,     // float 0.0–1.0  
  "evaluated\_at":     "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** overall\_verdict is exactly one of {PASS, FAIL, WARN}; every score is integer 1–5; overall\_score equals weighted mean of scores dict; rubric\_id matches an existing rubric file SHA-256.

## **Week 3 — Document Refinery  (extraction\_record)**

// File: outputs/week3/extractions.jsonl  
{  
  "doc\_id":       "uuid-v4",  
  "source\_path":  "absolute/path/or/https://url",  
  "source\_hash":  "sha256\_of\_source\_file",  
  "extracted\_facts": \[  
    {  
      "fact\_id":        "uuid-v4",  
      "text":           "string — the extracted fact in plain English",  
      "entity\_refs":    \["entity\_id\_1", "entity\_id\_2"\],  
      "confidence":     0.93,  // float MUST be 0.0–1.0  
      "page\_ref":       4,     // nullable int  
      "source\_excerpt": "verbatim text the fact was derived from"  
    }  
  \],  
  "entities": \[  
    {  
      "entity\_id":       "uuid-v4",  
      "name":            "string",  
      "type":            "PERSON",  // PERSON|ORG|LOCATION|DATE|AMOUNT|OTHER  
      "canonical\_value": "string"  
    }  
  \],  
  "extraction\_model": "claude-3-5-sonnet-20241022",  
  "processing\_time\_ms": 1240,  
  "token\_count": { "input": 4200, "output": 890 },  
  "extracted\_at": "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** confidence is float 0.0–1.0 (NOT 0–100); entity\_refs\[\] contains only IDs that exist in the entities\[\] of the same record; entity.type is one of the six enum values; processing\_time\_ms is a positive int.

## **Week 4 — Brownfield Cartographer  (lineage\_snapshot)**

// File: outputs/week4/lineage\_snapshots.jsonl  
{  
  "snapshot\_id":    "uuid-v4",  
  "codebase\_root":  "/absolute/path/to/repo",  
  "git\_commit":     "40-char-sha",  
  "nodes": \[  
    {  
      "node\_id":  "file::src/main.py",  // stable, colon-separated type::path  
      "type":     "FILE",  // FILE|TABLE|SERVICE|MODEL|PIPELINE|EXTERNAL  
      "label":    "main.py",  
      "metadata": {  
        "path":          "src/main.py",  
        "language":      "python",  
        "purpose":       "one-sentence LLM-inferred purpose",  
        "last\_modified": "2025-01-14T09:00:00Z"  
      }  
    }  
  \],  
  "edges": \[  
    {  
      "source":       "file::src/main.py",  
      "target":       "file::src/utils.py",  
      "relationship": "IMPORTS",  // IMPORTS|CALLS|READS|WRITES|PRODUCES|CONSUMES  
      "confidence":   0.95  
    }  
  \],  
  "captured\_at": "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** every edge.source and edge.target must reference a node\_id in the nodes\[\] array of the same snapshot; edge.relationship is one of the six enum values; git\_commit is exactly 40 hex characters.

## **Week 5 — Event Sourcing Platform  (event\_record)**

// File: outputs/week5/events.jsonl  
{  
  "event\_id":        "uuid-v4",  
  "event\_type":      "DocumentProcessed",  // PascalCase, registered in schema registry  
  "aggregate\_id":    "uuid-v4",  
  "aggregate\_type":  "Document",           // PascalCase  
  "sequence\_number": 42,                   // int, monotonically increasing per aggregate  
  "payload": {},                            // event-type-specific, must pass event schema  
  "metadata": {  
    "causation\_id":   "uuid-v4 | null",  
    "correlation\_id": "uuid-v4",  
    "user\_id":        "string",  
    "source\_service": "week3-document-refinery"  
  },  
  "schema\_version":  "1.0",  
  "occurred\_at":     "2025-01-15T14:23:00Z",  
  "recorded\_at":     "2025-01-15T14:23:01Z"  // must be \>= occurred\_at  
}

**Contract enforcement targets:** recorded\_at \>= occurred\_at; sequence\_number is monotonically increasing per aggregate\_id (no gaps, no duplicates); event\_type is PascalCase and registered in your event schema registry; payload validates against the event\_type's JSON Schema.

## **LangSmith Trace Export  (trace\_record)**

// Export via: langsmith export \--project your\_project \--format jsonl \> outputs/traces/runs.jsonl  
{  
  "id":             "uuid-v4",  
  "name":           "string — chain or LLM name",  
  "run\_type":       "llm",  // llm|chain|tool|retriever|embedding  
  "inputs":         {},  
  "outputs":        {},  
  "error":          null,    // string | null  
  "start\_time":     "2025-01-15T14:23:00Z",  
  "end\_time":       "2025-01-15T14:23:02Z",  
  "total\_tokens":   5090,  
  "prompt\_tokens":  4200,  
  "completion\_tokens": 890,  
  "total\_cost":     0.0153,  // float USD  
  "tags":           \["week3", "extraction"\],  
  "parent\_run\_id":  "uuid-v4 | null",  
  "session\_id":     "uuid-v4"  
}

**Contract enforcement targets:** end\_time \> start\_time; total\_tokens \= prompt\_tokens \+ completion\_tokens; run\_type is one of the five enum values; total\_cost \>= 0\. This contract is enforced by the AI Contract Extension in Phase 4\.
