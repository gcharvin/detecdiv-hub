from worker.preview_text import fit_text_scale


def test_fit_text_scale_shrinks_long_text_on_small_frames():
    text = "CH1 BRIGHTFIELD_10X"

    assert fit_text_scale(text, available_width=200, desired_scale=4) == 1
    assert fit_text_scale(text, available_width=1200, desired_scale=4) == 4


def test_fit_text_scale_honors_lower_bounds():
    assert fit_text_scale("", available_width=200, desired_scale=4) == 1
    assert fit_text_scale("FRAME 1", available_width=20, desired_scale=4) == 1
