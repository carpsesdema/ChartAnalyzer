# --- trend_analyzer.py ---

import pandas as pd
import numpy as np
import pytz  # Need pytz for timezone conversion here too

# --- Scipy Check ---
# Check for scipy presence, as it's crucial for this module
try:
    from scipy.signal import find_peaks

    _has_scipy = True
except ImportError:
    _has_scipy = False
    print(
        "Warning (trend_analyzer.py): 'scipy' library not found (pip install scipy). "
        "Trend line functions will not work."
    )
# --- End Scipy Check ---


def _find_swing_points(data_series, distance=5, prominence=None):
    """
    Internal helper finds peaks (highs) or troughs (lows) in a data series.

    Args:
        data_series (pd.Series): Price data (e.g., 'High' for peaks, -'Low' for troughs).
        distance (int): Minimum number of samples between peaks.
        prominence (float, optional): Required prominence of peaks.

    Returns:
        np.array: Indices of the peaks/troughs in the data_series.
                 Returns empty array if scipy is not available.
    """
    if not _has_scipy:
        return np.array([], dtype=int)

    if data_series is None or data_series.empty:
        return np.array([], dtype=int)

    # find_peaks finds maxima. To find minima (troughs), invert the series.
    try:
        peaks_indices, _ = find_peaks(
            data_series.values, distance=distance, prominence=prominence
        )
        return peaks_indices
    except Exception as e:
        print(f"Error in find_peaks: {e}")
        return np.array([], dtype=int)


def _generate_trend_line_segments(swing_indices, data, line_type="up"):
    """
    Internal helper generates candidate trend lines by connecting consecutive swing points.

    Args:
        swing_indices (np.array): Indices of swing highs or lows.
        data (pd.DataFrame): The full stock data DataFrame with a DatetimeIndex.
        line_type (str): 'up' (connect lows) or 'down' (connect highs).

    Returns:
        list: List of dictionaries, each representing a trend line segment.
              {'type', 'start_ts', 'start_p', 'end_ts', 'end_p'}
    """
    lines = []
    if len(swing_indices) < 2:
        return lines

    # Ensure index is datetime-like to get timestamps correctly
    if not isinstance(data.index, pd.DatetimeIndex):
        print(
            "Error (_generate_trend_line_segments): Data index must be a DatetimeIndex."
        )
        return lines

    price_col = "Low" if line_type == "up" else "High"
    if price_col not in data.columns:
        print(f"Error: Column '{price_col}' not found in DataFrame.")
        return lines

    # Get UTC timestamps (seconds since epoch) for consistency
    try:
        if hasattr(data.index, "tz") and data.index.tz is not None:
            timestamps = data.index.tz_convert(pytz.utc).astype(np.int64) // 10**9
        else:
            # Assume naive index is UTC or localize
            try:
                timestamps = data.index.tz_localize("UTC").astype(np.int64) // 10**9
            except TypeError:  # Already localized or other issue
                timestamps = (
                    data.index.astype(np.int64) // 10**9
                )  # Direct conversion (less safe)
    except Exception as ts_err:
        print(f"Error converting index to timestamps in trend analyzer: {ts_err}")
        return lines

    for i in range(len(swing_indices) - 1):
        idx1 = swing_indices[i]
        idx2 = swing_indices[i + 1]

        # Basic index bounds check
        if idx1 >= len(data) or idx2 >= len(data):
            print(
                f"Warning: Swing index out of bounds ({idx1}, {idx2} vs len {len(data)}). Skipping."
            )
            continue

        # Get timestamps and prices for the two points
        try:
            ts1 = timestamps[idx1]
            ts2 = timestamps[idx2]
            p1 = data[price_col].iloc[idx1]
            p2 = data[price_col].iloc[idx2]
        except IndexError:
            print(
                f"Warning: IndexError accessing data at swing indices ({idx1}, {idx2}). Skipping."
            )
            continue
        except Exception as e:
            print(f"Warning: Error accessing data for swing points: {e}. Skipping.")
            continue

        # Basic validation: Check slope direction
        if ts1 >= ts2:
            continue  # Time should advance

        # Avoid division by zero if timestamps are identical (shouldn't happen with distance > 0)
        time_delta = ts2 - ts1
        if time_delta == 0:
            continue

        slope = (p2 - p1) / time_delta

        valid = False
        if line_type == "up" and p2 >= p1:  # Uptrend: Higher or equal low
            valid = True
        elif line_type == "down" and p2 <= p1:  # Downtrend: Lower or equal high
            valid = True

        # --- Add More Filtering Here Later ---
        # - Minimum length (time or points)
        # - Check if price crosses the line significantly between points 1 and 2
        # - Minimum number of points (requires more complex logic than just segments)
        # --------------------------------

        if valid:
            lines.append(
                {
                    "type": line_type,
                    "start_ts": ts1,
                    "start_p": p1,
                    "end_ts": ts2,
                    "end_p": p2,
                }
            )
    return lines


def find_trend_lines(data, distance=5, prominence=None):
    """
    Finds potential uptrend and downtrend line segments in the provided stock data.

    Args:
        data (pd.DataFrame): Stock data with 'High', 'Low' columns and a DatetimeIndex.
        distance (int): Minimum number of data points between detected swing points.
                       Adjust based on data frequency (e.g., more for 1m, less for 1h).
        prominence (float, optional): Minimum vertical distance (in price units) for a
                                   swing point to be considered significant. Filters noise.

    Returns:
        list: A list of dictionaries, where each dictionary represents a trend line segment.
              Example: {'type': 'up', 'start_ts': ..., 'start_p': ..., 'end_ts': ..., 'end_p': ...}
              Returns empty list if scipy is not available or data is invalid.
    """
    if not _has_scipy:
        print("Scipy not found. Cannot execute find_trend_lines.")
        return []

    if not isinstance(data, pd.DataFrame) or data.empty:
        print("Invalid input: 'data' must be a non-empty pandas DataFrame.")
        return []

    required_cols = ["High", "Low"]
    if not all(col in data.columns for col in required_cols):
        print(f"Invalid input: DataFrame must contain columns: {required_cols}")
        return []

    if not isinstance(data.index, pd.DatetimeIndex):
        print("Invalid input: DataFrame index must be a DatetimeIndex.")
        return []

    all_lines = []

    # --- Find Uptrend Lines (connecting lows) ---
    # Find troughs by finding peaks in the *negative* low series
    lows = data["Low"]
    swing_low_indices = _find_swing_points(
        -lows, distance=distance, prominence=prominence
    )
    # print(f"DEBUG: Found {len(swing_low_indices)} swing lows.")
    if len(swing_low_indices) > 1:
        uptrend_lines = _generate_trend_line_segments(
            swing_low_indices, data, line_type="up"
        )
        all_lines.extend(uptrend_lines)
        # print(f"DEBUG: Generated {len(uptrend_lines)} uptrend segments.")

    # --- Find Downtrend Lines (connecting highs) ---
    highs = data["High"]
    swing_high_indices = _find_swing_points(
        highs, distance=distance, prominence=prominence
    )
    # print(f"DEBUG: Found {len(swing_high_indices)} swing highs.")
    if len(swing_high_indices) > 1:
        downtrend_lines = _generate_trend_line_segments(
            swing_high_indices, data, line_type="down"
        )
        all_lines.extend(downtrend_lines)
        # print(f"DEBUG: Generated {len(downtrend_lines)} downtrend segments.")

    # --- Future Enhancements ---
    # - Merge consecutive segments with similar slopes?
    # - Filter lines based on number of touches?
    # - Extend lines?
    # ---------------------------

    return all_lines


# --- Example Usage (for testing this module directly) ---
if __name__ == "__main__":
    print("Testing trend_analyzer module...")
    if not _has_scipy:
        print("Cannot run example: Scipy is required.")
    else:
        # Create some sample data (e.g., a sine wave with noise)
        index = pd.date_range(
            start="2023-01-01 09:30",
            periods=100,
            freq="5min",
            tz="America/New_York",
        )
        prices = (
            100
            + 5 * np.sin(np.linspace(0, 4 * np.pi, 100))
            + np.random.randn(100) * 0.5
        )
        highs = prices + np.random.rand(100) * 0.5 + 0.1
        lows = prices - np.random.rand(100) * 0.5 - 0.1
        opens = prices + (np.random.rand(100) - 0.5) * 0.2
        closes = prices + (np.random.rand(100) - 0.5) * 0.2
        volumes = np.random.randint(1000, 10000, 100)

        sample_df = pd.DataFrame(
            {
                "Open": opens,
                "High": highs,
                "Low": lows,
                "Close": closes,
                "Volume": volumes,
            },
            index=index,
        )

        print("Sample DataFrame head:")
        print(sample_df.head())

        # Find trend lines
        distance_param = 5
        prominence_param = 0.5  # Adjust prominence based on price scale
        print(
            f"\nFinding trends with distance={distance_param}, prominence={prominence_param}..."
        )

        trend_lines = find_trend_lines(
            sample_df, distance=distance_param, prominence=prominence_param
        )

        print(f"\nFound {len(trend_lines)} trend line segments:")
        if trend_lines:
            # Print first few lines as example
            for i, line in enumerate(trend_lines[:5]):
                start_dt = pd.to_datetime(
                    line["start_ts"], unit="s", utc=True
                ).tz_convert("America/New_York")
                end_dt = pd.to_datetime(line["end_ts"], unit="s", utc=True).tz_convert(
                    "America/New_York"
                )
                print(
                    f"  {i + 1}. Type: {line['type']}, "
                    f"Start: {start_dt.strftime('%H:%M')} @ {line['start_p']:.2f}, "
                    f"End: {end_dt.strftime('%H:%M')} @ {line['end_p']:.2f}"
                )
            if len(trend_lines) > 5:
                print("  ...")
        else:
            print("  (No trend lines found)")
