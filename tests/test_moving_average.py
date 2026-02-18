from decimal import Decimal

from app.services.moving_average import RollingWindowState, calculate_moving_average


def test_calculate_moving_average_returns_none_with_fewer_than_five_points() -> None:
    result = calculate_moving_average(
        [Decimal("100.0"), Decimal("101.0"), Decimal("102.0")],
        window_size=5,
    )
    assert result is None


def test_calculate_moving_average_returns_average_for_last_five_points() -> None:
    result = calculate_moving_average(
        [
            Decimal("100.0"),
            Decimal("101.0"),
            Decimal("102.0"),
            Decimal("103.0"),
            Decimal("104.0"),
        ],
        window_size=5,
    )
    assert result == Decimal("102.0")


def test_rolling_window_state_updates_in_o1_style() -> None:
    state = RollingWindowState(window_size=5)

    for value in [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4")]:
        sample_size, average = state.add_price(value)
        assert sample_size < 5
        assert average is None

    sample_size, average = state.add_price(Decimal("5"))
    assert sample_size == 5
    assert average == Decimal("3")

    sample_size, average = state.add_price(Decimal("6"))
    assert sample_size == 5
    assert average == Decimal("4")
