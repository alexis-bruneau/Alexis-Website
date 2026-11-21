from redfin.analytics.sale_curve import DEFAULT_MAX_DAYS, MIN_SAMPLE, sale_curve


def test_fewer_than_the_minimum_sales_gives_no_curve():
    assert sale_curve(list(range(MIN_SAMPLE - 1))) is None
    assert sale_curve([]) is None


def test_exactly_the_minimum_gives_a_curve():
    curve = sale_curve(list(range(MIN_SAMPLE)))
    assert curve is not None and curve["sample"] == MIN_SAMPLE


def test_share_is_the_fraction_sold_within_n_days():
    # 20 sales: 1..20 days on market
    curve = sale_curve(range(1, 21))
    share = curve["share"]
    assert len(share) == DEFAULT_MAX_DAYS + 1          # N = 0..180
    assert share[0] == 0.0                              # nothing sold in 0 days
    assert share[1] == 0.05                             # 1 of 20
    assert share[10] == 0.5
    assert share[20] == 1.0 and share[180] == 1.0
    assert share == sorted(share)                       # cumulative: never goes down


def test_day_zero_sales_count_at_n_zero():
    curve = sale_curve([0] * 5 + list(range(10, 25)))   # 20 sales, 5 of them sold on day 0
    assert curve["share"][0] == 0.25


def test_sales_past_the_window_stay_in_the_denominator():
    # 20 sales, 4 of them slower than 180 days: the curve tops out at 80%, it does not renormalise to 100%
    curve = sale_curve(list(range(1, 17)) + [200, 300, 400, 704])
    assert curve["sample"] == 20
    assert curve["share"][180] == 0.8


def test_missing_negative_and_nan_values_are_ignored():
    messy = list(range(1, 21)) + [None, float("nan"), -3]
    curve = sale_curve(messy)
    assert curve["sample"] == 20
    assert sale_curve(list(range(1, 20)) + [None, float("nan"), -3]) is None   # only 19 usable


def test_p50_and_p80_days():
    curve = sale_curve(range(1, 21))                    # n = 20: 10 sales by day 10, 16 by day 16
    assert curve["p50_days"] == 10
    assert curve["p80_days"] == 16


def test_p80_is_exact_when_float_noise_would_ask_for_one_more_sale():
    # n = 35 -> 0.8 * 35 is 28.000000000000004 in floating point; 28 sales must be enough
    curve = sale_curve(range(1, 36))
    assert curve["p80_days"] == 28


def test_p80_is_none_when_the_curve_never_reaches_it():
    curve = sale_curve(list(range(1, 11)) + [500] * 10)  # only half sell inside 180 days
    assert curve["p50_days"] == 10
    assert curve["p80_days"] is None


def test_input_order_does_not_matter_and_input_is_not_mutated():
    values = [30, 5, 90, 12, 7, 60, 45, 3, 22, 18, 40, 9, 11, 75, 14, 28, 33, 1, 66, 50]
    before = list(values)
    assert sale_curve(values) == sale_curve(sorted(values))
    assert values == before
