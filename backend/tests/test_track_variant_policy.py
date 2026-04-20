from __future__ import annotations

import unittest

from backend.app.track_variant_policy import (
    classify_variant_component,
    classify_edit_label,
    classify_mix_label,
    classify_score_soundtrack_label,
    classify_version_label,
    interpret_track_variant_title,
    is_groupable_edit_label,
    is_groupable_mix_label,
    is_groupable_score_soundtrack_label,
    is_groupable_version_label,
    load_track_variant_policy,
)


class TrackVariantPolicyTests(unittest.TestCase):
    def test_policy_loads_with_semantic_categories(self) -> None:
        policy = load_track_variant_policy()

        self.assertGreaterEqual(policy.model_version, 2)
        self.assertGreaterEqual(len(policy.families), 20)

        version = policy.get_family("version")
        self.assertIsNotNone(version)
        assert version is not None
        self.assertEqual("version_label_umbrella", version.semantic_category)
        self.assertTrue(version.needs_review)
        self.assertAlmostEqual(0.55, version.base_confidence)
        self.assertGreaterEqual(len(version.subtypes), 5)

    def test_review_families_include_mix_and_edit(self) -> None:
        policy = load_track_variant_policy()
        family_names = {item.family for item in policy.review_families}

        self.assertIn("edit", family_names)
        self.assertIn("mix", family_names)
        self.assertIn("version", family_names)

    def test_version_classification_uses_policy_semantics(self) -> None:
        single = classify_version_label("single version")
        alternate = classify_version_label("alternate version")
        named = classify_version_label("Natureboy Flako Version")
        remaster = classify_version_label("2006 Remastered Version")

        self.assertIsNotNone(single)
        self.assertIsNotNone(alternate)
        self.assertIsNotNone(named)
        self.assertIsNotNone(remaster)
        assert single is not None
        assert alternate is not None
        assert named is not None
        assert remaster is not None

        self.assertEqual("packaging_version", single.semantic_category)
        self.assertEqual("alternate_take_or_arrangement", alternate.semantic_category)
        self.assertEqual("attributed_derived_version", named.semantic_category)
        self.assertEqual("mastering_or_reissue_label", remaster.semantic_category)
        self.assertTrue(is_groupable_version_label("single version"))
        self.assertTrue(is_groupable_version_label("alternate version"))
        self.assertFalse(is_groupable_version_label("Natureboy Flako Version"))

    def test_edit_classification_uses_policy_semantics(self) -> None:
        generic = classify_edit_label("single edit")
        radio = classify_edit_label("radio edit")
        attributed = classify_edit_label("Local Natives Edit")

        self.assertIsNotNone(generic)
        self.assertIsNotNone(radio)
        self.assertIsNotNone(attributed)
        assert generic is not None
        assert radio is not None
        assert attributed is not None

        self.assertEqual("packaging_edit", generic.semantic_category)
        self.assertEqual("broadcast_length_or_content_edit", radio.semantic_category)
        self.assertEqual("attributed_derived_version", attributed.semantic_category)
        self.assertTrue(is_groupable_edit_label("single edit"))
        self.assertTrue(is_groupable_edit_label("radio edit"))
        self.assertFalse(is_groupable_edit_label("Local Natives Edit"))

    def test_variant_component_classification_handles_remix_first(self) -> None:
        remix = classify_variant_component("Quantic Remix")
        radio_edit = classify_variant_component("Radio Edit")

        self.assertIsNotNone(remix)
        self.assertIsNotNone(radio_edit)
        assert remix is not None
        assert radio_edit is not None
        self.assertEqual("remix", remix.family)
        self.assertFalse(remix.groupable_by_default)
        self.assertEqual("edit", radio_edit.family)
        self.assertTrue(radio_edit.groupable_by_default)

    def test_interpret_track_variant_title_detects_dominant_remix_with_secondary_edit(self) -> None:
        interpretation = interpret_track_variant_title("Puente Roto (Quantic Remix) - Radio Edit")

        self.assertEqual("Puente Roto", interpretation.base_title_anchor)
        self.assertEqual("remix", interpretation.dominant_family)
        self.assertEqual("attributed_derived_version", interpretation.dominant_semantic_category)
        self.assertEqual(2, len(interpretation.components))
        self.assertEqual(["edit", "remix"], [component.family for component in interpretation.components])
        self.assertEqual(
            ["radio edit", "quantic remix"],
            [component.normalized_label for component in interpretation.components],
        )

    def test_interpret_track_variant_title_handles_rerecorded_version(self) -> None:
        interpretation = interpret_track_variant_title("Heart Of Glass - Rerecorded 2014 Version")

        self.assertEqual("Heart Of Glass", interpretation.base_title_anchor)
        self.assertEqual("version", interpretation.dominant_family)
        self.assertEqual("recording_lineage_change", interpretation.dominant_semantic_category)
        self.assertEqual(1, len(interpretation.components))

    def test_interpret_track_variant_title_preserves_internal_hyphenated_titles(self) -> None:
        afro = interpret_track_variant_title("Afro-Caribbean Mixtape")
        kasalefkut = interpret_track_variant_title("Kasalefkut-hulu - Stereo Master")

        self.assertEqual("Afro-Caribbean Mixtape", afro.base_title_anchor)
        self.assertEqual(0, len(afro.components))
        self.assertEqual("Kasalefkut-hulu", kasalefkut.base_title_anchor)
        self.assertEqual(["format"], [component.family for component in kasalefkut.components])

    def test_interpret_track_variant_title_ignores_non_variant_parentheticals(self) -> None:
        sleepless = interpret_track_variant_title("An Illustration of Loneliness (Sleepless in New York)")
        numbered = interpret_track_variant_title("Closing (0037)")

        self.assertEqual("An Illustration of Loneliness (Sleepless in New York)", sleepless.base_title_anchor)
        self.assertEqual(0, len(sleepless.components))
        self.assertEqual("Closing (0037)", numbered.base_title_anchor)
        self.assertEqual(0, len(numbered.components))

    def test_mix_classification_uses_policy_semantics(self) -> None:
        original = classify_mix_label("original mix")
        radio = classify_mix_label("radio mix")
        stereo = classify_mix_label("2021 stereo mix")
        attributed = classify_mix_label("Spike Stent Mix")

        self.assertIsNotNone(original)
        self.assertIsNotNone(radio)
        self.assertIsNotNone(stereo)
        self.assertIsNotNone(attributed)
        assert original is not None
        assert radio is not None
        assert stereo is not None
        assert attributed is not None

        self.assertEqual("base_release_mix_label", original.semantic_category)
        self.assertEqual("broadcast_mix_treatment", radio.semantic_category)
        self.assertEqual("format_or_presentation_change", stereo.semantic_category)
        self.assertEqual("attributed_derived_version", attributed.semantic_category)
        self.assertTrue(is_groupable_mix_label("original mix"))
        self.assertTrue(is_groupable_mix_label("radio mix"))
        self.assertTrue(is_groupable_mix_label("new stereo mix"))
        self.assertTrue(is_groupable_mix_label("2021 stereo mix"))
        self.assertTrue(is_groupable_mix_label("extended mix"))
        self.assertTrue(is_groupable_mix_label("ambient mix"))
        self.assertFalse(is_groupable_mix_label("Spike Stent Mix"))

    def test_interpret_track_variant_title_detects_dominant_mix_with_secondary_edit(self) -> None:
        interpretation = interpret_track_variant_title("Deadcrush - Spike Stent Mix - Radio Edit")

        self.assertEqual("Deadcrush", interpretation.base_title_anchor)
        self.assertEqual("mix", interpretation.dominant_family)
        self.assertEqual("attributed_derived_version", interpretation.dominant_semantic_category)
        self.assertEqual(2, len(interpretation.components))
        self.assertEqual(["edit", "mix"], [component.family for component in interpretation.components])
        self.assertEqual(
            ["radio edit", "spike stent mix"],
            [component.normalized_label for component in interpretation.components],
        )

    def test_year_and_soundtrack_classification_use_policy_semantics(self) -> None:
        year = classify_variant_component("1973")
        placement = classify_score_soundtrack_label('From "Better Call Saul"')
        score = classify_score_soundtrack_label('From "Eternal Sunshine of the Spotless Mind"/Score')

        self.assertIsNotNone(year)
        self.assertIsNotNone(placement)
        self.assertIsNotNone(score)
        assert year is not None
        assert placement is not None
        assert score is not None

        self.assertEqual("year_tag", year.family)
        self.assertTrue(year.groupable_by_default)
        self.assertEqual("placement_or_context_label", placement.semantic_category)
        self.assertEqual("score_or_cue_context", score.semantic_category)
        self.assertTrue(is_groupable_score_soundtrack_label('From "Better Call Saul"'))
        self.assertFalse(is_groupable_score_soundtrack_label('From "Eternal Sunshine of the Spotless Mind"/Score'))


if __name__ == "__main__":
    unittest.main()
