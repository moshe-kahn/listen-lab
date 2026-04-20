from __future__ import annotations

from backend.app.track_variant_policy import load_track_variant_policy


def main() -> None:
    policy = load_track_variant_policy()
    print(f"Track Variant Policy v{policy.model_version}")
    print(policy.description)
    print("")
    for family in policy.families:
        print(
            f"{family.family}: semantic={family.semantic_category}, "
            f"same_default={family.same_composition_default}, "
            f"confidence={family.base_confidence:.2f}, "
            f"separate_default={family.separate_default}, "
            f"review={family.needs_review}"
        )
        if family.example_labels:
            print(f"  examples: {', '.join(family.example_labels)}")
        for subtype in family.subtypes:
            print(
                f"  - {subtype.observed_subfamily}: semantic={subtype.semantic_category}"
            )
            if subtype.example_labels:
                print(f"    examples: {', '.join(subtype.example_labels)}")


if __name__ == "__main__":
    main()
