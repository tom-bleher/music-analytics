#!/usr/bin/env python3
"""
Audio Analyzer for Music Analytics

Uses librosa to extract Spotify-like audio features from audio files.
Features computed:
- tempo: BPM of the track
- energy: Intensity and activity (0.0-1.0)
- danceability: How suitable for dancing (0.0-1.0)
- valence: Musical positivity/happiness (0.0-1.0)
- acousticness: Confidence of acoustic sound (0.0-1.0)
- instrumentalness: Prediction of no vocals (0.0-1.0)
- speechiness: Presence of spoken words (0.0-1.0)
- loudness: Overall loudness in dB (typically -60 to 0)
- key: Pitch class (0=C, 1=C#, ..., 11=B)
- mode: Major (1) or minor (0)
- time_signature: Estimated beats per bar (typically 3, 4, or 6)
"""

import os
import warnings
from pathlib import Path
from typing import Dict, Optional, List

import numpy as np

# Suppress librosa warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

import librosa

# Supported audio extensions
AUDIO_EXTENSIONS = {'.mp3', '.flac', '.ogg', '.wav', '.m4a', '.aac', '.wma', '.opus'}


def analyze_file(file_path: str) -> Optional[Dict]:
    """
    Analyze an audio file and extract Spotify-like features.

    Args:
        file_path: Path to the audio file

    Returns:
        Dictionary of audio features, or None if analysis fails
    """
    try:
        # Load audio file
        y, sr = librosa.load(file_path, sr=22050, mono=True, duration=180)  # First 3 min

        if len(y) == 0:
            return None

        # Basic feature extraction
        features = {}

        # Tempo (BPM)
        tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr)
        features['tempo'] = float(tempo) if not hasattr(tempo, '__len__') else float(tempo[0])

        # Compute spectral features
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
        spectral_bandwidth = librosa.feature.spectral_bandwidth(y=y, sr=sr)[0]
        spectral_contrast = librosa.feature.spectral_contrast(y=y, sr=sr)
        mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)

        # RMS energy
        rms = librosa.feature.rms(y=y)[0]

        # Zero crossing rate (useful for speechiness)
        zcr = librosa.feature.zero_crossing_rate(y)[0]

        # Chroma features for key detection
        chroma = librosa.feature.chroma_cqt(y=y, sr=sr)

        # Harmonic and percussive separation
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        harmonic_ratio = np.sum(np.abs(y_harmonic)) / (np.sum(np.abs(y)) + 1e-10)
        percussive_ratio = np.sum(np.abs(y_percussive)) / (np.sum(np.abs(y)) + 1e-10)

        # === Energy (0.0-1.0) ===
        # Based on RMS energy, spectral bandwidth, and overall loudness
        rms_mean = np.mean(rms)
        rms_normalized = min(1.0, rms_mean / 0.15)  # Normalize to typical range
        bandwidth_normalized = min(1.0, np.mean(spectral_bandwidth) / 3000)
        energy = 0.6 * rms_normalized + 0.4 * bandwidth_normalized
        features['energy'] = float(np.clip(energy, 0, 1))

        # === Danceability (0.0-1.0) ===
        # Based on tempo regularity, beat strength, and groove
        # Ideal dance tempo is around 120 BPM
        tempo_score = 1.0 - abs(features['tempo'] - 120) / 120
        tempo_score = max(0, tempo_score)

        # Beat strength from onset envelope
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        beat_strength = np.mean(onset_env[beat_frames]) if len(beat_frames) > 0 else 0
        beat_strength_normalized = min(1.0, beat_strength / 10)

        # Rhythm regularity (variance in beat intervals)
        if len(beat_frames) > 1:
            beat_intervals = np.diff(librosa.frames_to_time(beat_frames, sr=sr))
            rhythm_regularity = 1.0 - min(1.0, np.std(beat_intervals) * 5)
        else:
            rhythm_regularity = 0.5

        danceability = 0.3 * tempo_score + 0.4 * beat_strength_normalized + 0.3 * rhythm_regularity
        features['danceability'] = float(np.clip(danceability, 0, 1))

        # === Valence (0.0-1.0) ===
        # Musical positivity - higher values = happier
        # Based on mode (major/minor), spectral centroid, and tempo
        chroma_mean = np.mean(chroma, axis=1)
        major_template = np.array([1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1])  # Major scale
        minor_template = np.array([1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0])  # Minor scale

        # Find best key fit
        best_major_corr = 0
        best_minor_corr = 0
        best_key = 0
        for shift in range(12):
            major_shifted = np.roll(major_template, shift)
            minor_shifted = np.roll(minor_template, shift)
            major_corr = np.corrcoef(chroma_mean, major_shifted)[0, 1]
            minor_corr = np.corrcoef(chroma_mean, minor_shifted)[0, 1]
            if major_corr > best_major_corr:
                best_major_corr = major_corr
                best_key = shift
            if minor_corr > best_minor_corr:
                best_minor_corr = minor_corr

        mode_score = 1.0 if best_major_corr > best_minor_corr else 0.3

        # Brightness from spectral centroid
        brightness = min(1.0, np.mean(spectral_centroids) / 4000)

        # Tempo contribution (faster = more energetic/positive feel)
        tempo_positivity = min(1.0, features['tempo'] / 150)

        valence = 0.4 * mode_score + 0.3 * brightness + 0.3 * tempo_positivity
        features['valence'] = float(np.clip(valence, 0, 1))

        # === Acousticness (0.0-1.0) ===
        # Low spectral centroid, low zero crossing rate, high dynamic range
        centroid_normalized = np.mean(spectral_centroids) / 5000
        acousticness = 1.0 - min(1.0, centroid_normalized)
        # Acoustic music tends to have less high frequency content
        rolloff_normalized = np.mean(spectral_rolloff) / 10000
        acousticness = 0.6 * acousticness + 0.4 * (1.0 - min(1.0, rolloff_normalized))
        features['acousticness'] = float(np.clip(acousticness, 0, 1))

        # === Instrumentalness (0.0-1.0) ===
        # Vocal detection - vocals have specific frequency characteristics
        # Check for vocal-like frequencies (85-255 Hz fundamental, formants up to 4000 Hz)
        # Use MFCC variance - vocals have more varied MFCCs
        mfcc_var = np.var(mfccs[1:5], axis=1).mean()  # Skip first MFCC (energy)
        mfcc_var_normalized = min(1.0, mfcc_var / 500)

        # Vocals have specific spectral characteristics
        # Higher zcr indicates more voiced content
        zcr_mean = np.mean(zcr)

        # Instrumentalness is inverse of vocal indicators
        vocal_indicator = 0.6 * mfcc_var_normalized + 0.4 * min(1.0, zcr_mean * 10)
        instrumentalness = 1.0 - vocal_indicator
        features['instrumentalness'] = float(np.clip(instrumentalness, 0, 1))

        # === Speechiness (0.0-1.0) ===
        # Spoken words have high zero-crossing rate and specific rhythm
        zcr_score = min(1.0, np.mean(zcr) * 15)

        # Speech has less tonal content
        spectral_flatness = librosa.feature.spectral_flatness(y=y)[0]
        flatness_score = np.mean(spectral_flatness)

        # Check for speech-like rhythm (syllables ~3-5 per second)
        onset_rate = len(librosa.onset.onset_detect(y=y, sr=sr)) / (len(y) / sr)
        speech_rhythm_score = 1.0 - abs(onset_rate - 4) / 4
        speech_rhythm_score = max(0, speech_rhythm_score)

        speechiness = 0.4 * zcr_score + 0.3 * flatness_score + 0.3 * speech_rhythm_score
        features['speechiness'] = float(np.clip(speechiness, 0, 1))

        # === Loudness (dB) ===
        # Convert RMS to dB
        rms_db = librosa.amplitude_to_db(rms, ref=np.max)
        features['loudness'] = float(np.mean(rms_db))

        # === Key (0-11) ===
        features['key'] = int(best_key)

        # === Mode (0 or 1) ===
        features['mode'] = 1 if best_major_corr > best_minor_corr else 0

        # === Time Signature ===
        # Estimate based on beat groupings
        if len(beat_frames) > 2:
            # Look for strong beats at regular intervals
            onset_env = librosa.onset.onset_strength(y=y, sr=sr)

            # Try different time signatures
            time_sig_scores = {}
            for ts in [3, 4, 6]:
                # Group beats and check for pattern
                groups = len(beat_frames) // ts
                if groups > 0:
                    first_beats = beat_frames[::ts]
                    strengths = onset_env[first_beats[:groups]] if len(first_beats[:groups]) > 0 else np.array([0])
                    time_sig_scores[ts] = np.mean(strengths)

            # Default to 4/4 if can't determine
            if time_sig_scores:
                features['time_signature'] = max(time_sig_scores, key=time_sig_scores.get)
            else:
                features['time_signature'] = 4
        else:
            features['time_signature'] = 4

        return features

    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")
        return None


def find_audio_files(music_dir: str) -> List[str]:
    """
    Find all audio files in a directory recursively.

    Args:
        music_dir: Path to music directory

    Returns:
        List of audio file paths
    """
    audio_files = []
    music_path = Path(music_dir)

    if not music_path.exists():
        print(f"Directory not found: {music_dir}")
        return []

    for ext in AUDIO_EXTENSIONS:
        audio_files.extend(music_path.rglob(f"*{ext}"))
        audio_files.extend(music_path.rglob(f"*{ext.upper()}"))

    return [str(f) for f in audio_files]


def analyze_library(music_dir: str, progress_callback=None) -> Dict[str, Dict]:
    """
    Analyze all audio files in a music library.

    Args:
        music_dir: Path to music directory
        progress_callback: Optional callback(current, total, file_path)

    Returns:
        Dictionary mapping file paths to their features
    """
    import db

    audio_files = find_audio_files(music_dir)
    total = len(audio_files)

    if total == 0:
        print(f"No audio files found in {music_dir}")
        return {}

    results = {}
    analyzed = 0
    skipped = 0
    failed = 0

    print(f"Found {total} audio files to analyze")

    for i, file_path in enumerate(audio_files):
        # Check if already analyzed (caching)
        if db.is_file_analyzed(file_path):
            skipped += 1
            if progress_callback:
                progress_callback(i + 1, total, file_path, "skipped")
            continue

        # Analyze the file
        features = analyze_file(file_path)

        if features:
            # Save to database
            db.save_audio_features(file_path, features)
            results[file_path] = features
            analyzed += 1
            if progress_callback:
                progress_callback(i + 1, total, file_path, "analyzed")
        else:
            failed += 1
            if progress_callback:
                progress_callback(i + 1, total, file_path, "failed")

        # Progress output
        if (i + 1) % 10 == 0 or i == total - 1:
            print(f"Progress: {i + 1}/{total} files (analyzed: {analyzed}, skipped: {skipped}, failed: {failed})")

    print(f"\nAnalysis complete!")
    print(f"  Analyzed: {analyzed} files")
    print(f"  Skipped (cached): {skipped} files")
    print(f"  Failed: {failed} files")

    return results


def get_feature_summary(file_path: str) -> Optional[str]:
    """
    Get a human-readable summary of audio features for a file.

    Args:
        file_path: Path to the audio file

    Returns:
        Formatted string summary
    """
    import db

    features = db.get_audio_features(file_path)
    if not features:
        return None

    key_names = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
    mode_name = 'Major' if features['mode'] == 1 else 'Minor'
    key_name = key_names[features['key']] if features['key'] is not None else 'Unknown'

    return f"""Audio Features for: {Path(file_path).name}
  Tempo:            {features['tempo']:.1f} BPM
  Key:              {key_name} {mode_name}
  Time Signature:   {features['time_signature']}/4
  Energy:           {features['energy']:.2f}
  Danceability:     {features['danceability']:.2f}
  Valence:          {features['valence']:.2f}
  Acousticness:     {features['acousticness']:.2f}
  Instrumentalness: {features['instrumentalness']:.2f}
  Speechiness:      {features['speechiness']:.2f}
  Loudness:         {features['loudness']:.1f} dB
  Analyzed at:      {features['analyzed_at']}
"""


def display_library_audio_stats():
    """Display aggregate statistics about analyzed audio files."""
    import db

    all_features = db.get_all_audio_features()

    if not all_features:
        print("No audio files have been analyzed yet.")
        print("Run: music-stats --analyze /path/to/music")
        return

    print(f"\n{'=' * 50}")
    print("  AUDIO FEATURES SUMMARY")
    print('=' * 50)

    print(f"\n  Total analyzed files: {len(all_features)}")

    # Calculate averages
    tempos = [f['tempo'] for f in all_features if f['tempo']]
    energies = [f['energy'] for f in all_features if f['energy']]
    danceabilities = [f['danceability'] for f in all_features if f['danceability']]
    valences = [f['valence'] for f in all_features if f['valence']]

    if tempos:
        print(f"\n  Average Tempo:        {np.mean(tempos):.1f} BPM (range: {min(tempos):.0f}-{max(tempos):.0f})")
    if energies:
        print(f"  Average Energy:       {np.mean(energies):.2f}")
    if danceabilities:
        print(f"  Average Danceability: {np.mean(danceabilities):.2f}")
    if valences:
        print(f"  Average Valence:      {np.mean(valences):.2f}")

    # Key distribution
    keys = [f['key'] for f in all_features if f['key'] is not None]
    modes = [f['mode'] for f in all_features if f['mode'] is not None]

    if keys:
        key_names = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
        from collections import Counter
        key_counts = Counter(keys)
        most_common_key = key_counts.most_common(1)[0]
        print(f"\n  Most common key:      {key_names[most_common_key[0]]} ({most_common_key[1]} tracks)")

    if modes:
        major_count = sum(1 for m in modes if m == 1)
        minor_count = len(modes) - major_count
        print(f"  Major vs Minor:       {major_count} major, {minor_count} minor")

    print()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isfile(path):
            # Analyze single file
            print(f"Analyzing: {path}")
            features = analyze_file(path)
            if features:
                for k, v in features.items():
                    print(f"  {k}: {v}")
        elif os.path.isdir(path):
            # Analyze library
            analyze_library(path)
    else:
        print("Usage: python audio_analyzer.py <file_or_directory>")
        print("\nSupported formats:", ", ".join(AUDIO_EXTENSIONS))
