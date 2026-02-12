import json
import re

def verify_pronouns(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    pronouns = ["it", "that", "this field", "that table"]
    failures = []
    
    for item in data:
        qid = item['id']
        original = item['generated_perturbations']['original']['nl_prompt']
        
        for p in item['generated_perturbations']['single_perturbations']:
            if p['perturbation_name'] == 'ambiguous_pronouns' and p['applicable']:
                perturbed = p['perturbed_nl_prompt']
                if not perturbed: continue
                
                # Check each pronoun
                for pron in pronouns:
                    if re.search(r'\b' + re.escape(pron) + r'\b', perturbed, re.I):
                        # If the pronoun exists in perturbed but not in original (or original didn't have 2+ mentions of the entity)
                        # We need to find what was REPLACED.
                        # Simplest check: does the perturbed prompt have fewer entity mentions?
                        if pron not in original:
                            # It's a new pronoun. We need to ensure there is a clear antecedent.
                            # The antecedent must appear EARLIER in the perturbed string.
                            pron_pos = perturbed.lower().find(pron.lower())
                            prefix = perturbed[:pron_pos]
                            
                            # This is a bit heuristic, but let's see if the prefix contains any typical entity names
                            # In our case, we can check if it contains any words from the original that are MISSING in the perturbed.
                            missing_words = [w for w in original.lower().split() if w not in perturbed.lower().split()]
                            
                            has_antecedent = False
                            for mw in missing_words:
                                if mw in prefix.lower():
                                    has_antecedent = True
                                    break
                            
                            if not has_antecedent:
                                failures.append((qid, perturbed))
    
    return failures

if __name__ == "__main__":
    file = 'dataset/current/nl_social_media_queries_systematic_20.json'
    results = verify_pronouns(file)
    if results:
        print(f"Found {len(results)} potential pronoun failures:")
        for qid, text in results:
            print(f"ID {qid}: {text}")
    else:
        print("No obvious pronoun failures found!")
