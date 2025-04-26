# --- playback_generator.py ---

import os
import tempfile
import time
import traceback

import imageio # For creating GIF/Video
import numpy as np
import pandas as pd
from PyQt6.QtCore import QObject, pyqtSignal, QThread # For threading and signals
import threading # For cancellation flag

# --- Constants ---
# Define steps and frame durations for different speeds
# (Lower frame duration = faster playback)
# (Higher step size = faster playback, fewer frames)
PLAYBACK_SPEEDS = {
    "Slow": {"step_bars": 1, "frame_duration_ms": 200},   # Add 1 bar per frame, 0.2s per frame
    "Normal": {"step_bars": 2, "frame_duration_ms": 100}, # Add 2 bars per frame, 0.1s per frame
    "Fast": {"step_bars": 5, "frame_duration_ms": 50},    # Add 5 bars per frame, 0.05s per frame
    "Very Fast": {"step_bars": 10, "frame_duration_ms": 50},# Add 10 bars per frame, 0.05s per frame
}
DEFAULT_PLAYBACK_SPEED = "Normal"

class PlaybackGeneratorWorker(QObject):
    """
    Worker object to handle playback generation in a separate thread.
    Communicates with the main GUI thread via signals.
    """
    # --- Signals ---
    # Signal to request the main thread updates its view and exports a frame
    # Args: frame_number, start_timestamp, end_timestamp, temp_frame_path
    request_export_frame = pyqtSignal(int, float, float, str)

    # Signal for progress update
    # Args: percentage (0-100)
    progress = pyqtSignal(int)

    # Signal when generation is finished or cancelled/errored
    # Args: message (success file path or error description)
    finished = pyqtSignal(str)

    def __init__(self, stock_data_df, output_filename, speed_setting, interval_seconds, cancel_event):
        """
        Args:
            stock_data_df (pd.DataFrame): DataFrame containing the full chart data.
            output_filename (str): Path where the final GIF/video will be saved.
            speed_setting (str): Key from PLAYBACK_SPEEDS (e.g., "Normal").
            interval_seconds (int): The approximate duration of one bar in seconds.
            cancel_event (threading.Event): Event object to signal cancellation.
        """
        super().__init__()
        self.stock_data_df = stock_data_df
        self.output_filename = output_filename
        self.speed_setting = speed_setting if speed_setting in PLAYBACK_SPEEDS else DEFAULT_PLAYBACK_SPEED
        self.interval_seconds = max(interval_seconds, 1) # Avoid division by zero
        self.cancel_event = cancel_event
        self._frame_paths = [] # List to store paths of generated frames
        self._is_running = True # Internal flag, less critical now with cancel_event

    def run(self):
        """Main generation logic executed by the thread."""
        print("Playback generation started...")
        self._is_running = True
        self._frame_paths = []
        temp_dir = None

        try:
            if self.stock_data_df is None or self.stock_data_df.empty:
                raise ValueError("No stock data available for playback.")
            if not self.output_filename:
                raise ValueError("Output filename not specified.")

            # --- Setup ---
            speed_params = PLAYBACK_SPEEDS[self.speed_setting]
            step_bars = speed_params["step_bars"]
            frame_duration_sec = speed_params["frame_duration_ms"] / 1000.0

            # Get timestamps from the DataFrame index (ensure it's datetime)
            if not isinstance(self.stock_data_df.index, pd.DatetimeIndex):
                 raise TypeError("Stock data index must be DatetimeIndex for playback.")

            # Convert index to UTC timestamps (seconds) for calculations
            try:
                if hasattr(self.stock_data_df.index, 'tz') and self.stock_data_df.index.tz is not None:
                    timestamps = self.stock_data_df.index.tz_convert('UTC').astype(np.int64) // 10**9
                else: # Assume or localize to UTC
                    timestamps = self.stock_data_df.index.tz_localize('UTC', ambiguous='infer').astype(np.int64) // 10**9
            except Exception as ts_err:
                raise ValueError(f"Failed to get timestamps from data index: {ts_err}")


            full_start_ts = timestamps[0]
            full_end_ts = timestamps[-1]
            total_bars = len(self.stock_data_df)

            # Determine the number of initial bars to show (e.g., first 10 or 5%?)
            initial_bars = max(10, total_bars // 20) # Show at least 10 bars initially

            # Create a temporary directory for frames
            temp_dir = tempfile.mkdtemp(prefix="chart_playback_")
            print(f"Using temporary directory for frames: {temp_dir}")

            # --- Frame Generation Loop ---
            num_frames = (total_bars - initial_bars + step_bars -1) // step_bars # Calculate total frames needed
            frame_count = 0

            for i in range(initial_bars, total_bars + step_bars, step_bars):
                if self.cancel_event.is_set():
                    print("Playback generation cancelled.")
                    self.finished.emit("Generation cancelled.")
                    self._is_running = False
                    break # Exit the loop

                current_bar_index = min(i, total_bars - 1) # Clamp to last bar index
                frame_end_ts = timestamps[current_bar_index]

                # Define path for this frame
                frame_filename = os.path.join(temp_dir, f"frame_{frame_count:05d}.png")
                self._frame_paths.append(frame_filename)

                # Emit signal to main thread to update view and export this frame
                # The main thread will handle plot updates and exporting
                self.request_export_frame.emit(frame_count, full_start_ts, frame_end_ts, frame_filename)

                # --- Wait briefly ---
                # This allows the main thread time to process the request.
                # A more robust solution might use QWaitCondition, but time.sleep is simpler here.
                # Adjust sleep time if needed, but keep it short.
                QThread.msleep(20) # Sleep for 20 milliseconds

                # Update progress
                progress_percent = int((frame_count / max(1, num_frames)) * 100)
                self.progress.emit(progress_percent)
                frame_count += 1


            if not self._is_running: # If cancelled during loop
                 raise InterruptedError("Playback cancelled")


            # --- Combine Frames ---
            if not self._frame_paths:
                raise ValueError("No frames were generated.")

            self.progress.emit(100) # Ensure progress reaches 100%
            print(f"Combining {len(self._frame_paths)} frames into {self.output_filename}...")

            # Use imageio to create the GIF/video
            # duration is per frame in seconds
            # loop=0 means loop forever for GIF
            # Use fps for video formats
            file_ext = os.path.splitext(self.output_filename)[1].lower()
            kwargs = {}
            if file_ext == '.gif':
                kwargs['duration'] = frame_duration_sec
                kwargs['loop'] = 0
            else: # Assume video format
                # Calculate FPS: More bars per frame or shorter duration = higher FPS
                # fps = 1.0 / frame_duration_sec # Basic FPS
                # Consider step size: if step=5, maybe fps should be higher?
                fps = max(1, int(step_bars / frame_duration_sec / step_bars * 10)) # Heuristic adjustment
                kwargs['fps'] = fps
                kwargs['quality'] = 8 # Decent quality (0-10), affects file size

            print(f"Using imageio kwargs: {kwargs}")

            with imageio.get_writer(self.output_filename, mode='I', **kwargs) as writer:
                for frame_path in self._frame_paths:
                    if self.cancel_event.is_set(): # Check again before reading/writing
                         raise InterruptedError("Playback cancelled during saving")
                    try:
                        image = imageio.imread(frame_path)
                        writer.append_data(image)
                    except FileNotFoundError:
                         print(f"Warning: Frame not found: {frame_path}. Skipping.")
                    except Exception as img_err:
                         print(f"Warning: Error reading/writing frame {frame_path}: {img_err}. Skipping.")


            self.finished.emit(f"Playback saved successfully:\n{self.output_filename}")
            print("Playback generation finished successfully.")

        except InterruptedError:
             # Already handled cancellation message emission inside loop/saving
             pass
        except Exception as e:
            print(f"ERROR during playback generation: {e}")
            traceback.print_exc() # Print full traceback for debugging
            self.finished.emit(f"Error during playback generation: {e}")
        finally:
            # --- Cleanup ---
            if temp_dir and os.path.exists(temp_dir):
                print(f"Cleaning up temporary directory: {temp_dir}")
                try:
                    # Remove individual frames first
                    for frame_path in self._frame_paths:
                        if os.path.exists(frame_path):
                            os.remove(frame_path)
                    # Remove the directory itself
                    os.rmdir(temp_dir)
                except Exception as cleanup_err:
                    print(f"Warning: Failed to fully clean up temp directory {temp_dir}: {cleanup_err}")
            self._is_running = False

    def stop(self):
        """Method to signal cancellation via the event."""
        print("Attempting to stop playback generation...")
        self.cancel_event.set() # Signal the loop/saving process to stop