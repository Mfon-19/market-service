from decimal import Decimal

from app.services.moving_average import calculate_moving_average


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
