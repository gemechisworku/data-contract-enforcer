# Step 1: find the current confidence distribution in your extractions
import json, statistics
with open('outputs/migrate/week3/extractions.jsonl') as f:
    facts = [json.loads(l) for l in f]
confs = [f2['confidence'] for f in facts for f2 in f.get('extracted_facts', [])]
print(f'min={min(confs):.3f} max={max(confs):.3f} mean={statistics.mean(confs):.3f}')
# If max > 1.0, your data already has the scale problem. Document it.
