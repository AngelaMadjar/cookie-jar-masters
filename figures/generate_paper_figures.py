"""
Regenerate paper bar-chart figures matching the original mean_rt_bidirectional style.
Exact colour palette and layout — bar value annotations removed.
"""
import os, sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(BASE, "docs", "results")
OUT     = os.path.join(BASE, "figures")

EXPERIMENTS = ["e1", "e2", "e3", "e4"]
# Original legend order (top→bottom) = bar order (left→right) within each TC group
EXP_LABELS  = ["E1 (Local)", "E2 (HTTP)", "E3 (Eventarc)", "E4 (Tasks)"]
# Exact hex colours from the original graphs
COLORS      = ["#2cb5ae", "#1a3558", "#d97045", "#7bb8d0"]

# Test cases shown in graphs (T1 excluded — outside systematic grid)
TEST_CASES  = ["t2", "t3", "t4", "t5", "t6", "t7"]

# X positions: pairs (T2/T3, T4/T5, T6/T7) are 1 unit apart within each scale group;
# 1.8 units gap between scale groups — reproduces original bidirectional layout
X_POSITIONS = np.array([0, 1,   2.8, 3.8,   5.6, 6.6])
SCALE_GROUPS = [
    ("Medium",  0, 1),
    ("Large",   2.8, 3.8),
    ("Skewed",  5.6, 6.6),
]
TC_XLABELS = ["T2\n(exist.)", "T3\n(new)", "T4\n(exist.)", "T5\n(new)", "T6\n(exist.)", "T7\n(new)"]

BAR_WIDTH  = 0.18
BAR_OFFSETS = np.array([-1.5, -0.5, 0.5, 1.5]) * BAR_WIDTH

# Match original image proportions (1412×723 px @ ~150 dpi)
FIGSIZE = (9.4, 5.6)

rcParams.update({
    "font.family":    "sans-serif",
    "font.size":      14,
    "axes.labelsize": 14,
    "axes.linewidth": 0.8,
    "xtick.labelsize": 13,
    "ytick.labelsize": 13,
})


def load_data():
    data = {}
    for exp in EXPERIMENTS:
        data[exp] = {}
        for tc in TEST_CASES:
            path = os.path.join(RESULTS, exp, f"{exp}_{tc}", "aggregated", "summary.csv")
            data[exp][tc] = pd.read_csv(path).iloc[0]
    return data


def extract(data, col):
    return {exp: [data[exp][tc][col] for tc in TEST_CASES] for exp in EXPERIMENTS}


def plot(values, ylabel, filename, figsize=None, group_fontsize=13, out_suffix="",
         ylim_extend=1.10, label_y_frac=1.02, legend_y=0.82, fixed_margins=None,
         ytick_nbins=None):
    fig, ax = plt.subplots(figsize=figsize or FIGSIZE)

    for i, (exp, label, color) in enumerate(zip(EXPERIMENTS, EXP_LABELS, COLORS)):
        ax.bar(X_POSITIONS + BAR_OFFSETS[i],
               values[exp],
               BAR_WIDTH,
               label=label,
               color=color,
               zorder=3)

    # Dashed vertical separators between scale groups
    for sep_x in [1.9, 4.7]:
        ax.axvline(sep_x, color="#aaaaaa", linestyle="--", linewidth=0.9, zorder=2)

    if ytick_nbins is not None:
        import matplotlib.ticker as _ticker
        ax.yaxis.set_major_locator(_ticker.MaxNLocator(nbins=ytick_nbins))

    # Y-axis gridlines (horizontal, behind bars)
    ax.yaxis.grid(True, color="#dddddd", linewidth=0.7, zorder=1)
    ax.set_axisbelow(True)

    # Scale group labels centred above each pair
    # Extend y-axis top by 18% so labels never overlap with tallest bar
    ylim_top = ax.get_ylim()[1]
    ax.set_ylim(top=ylim_top * ylim_extend)
    for grp_label, x0, x1 in SCALE_GROUPS:
        ax.text((x0 + x1) / 2, ylim_top * label_y_frac,
                grp_label, ha="center", va="top",
                fontsize=group_fontsize, color="#555555", style="italic")

    ax.set_xticks(X_POSITIONS)
    ax.set_xticklabels(TC_XLABELS)
    ax.set_ylabel(ylabel)
    ax.set_xlim(-0.55, 7.2)

    # Remove top and right spines (match original clean style)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.legend(loc="upper right", fontsize=13, framealpha=0.9,
              bbox_to_anchor=(1.0, legend_y))

    if fixed_margins is not None:
        # Use identical axes-rectangle margins (fraction of figure) across figures
        # instead of bbox_inches="tight", so stacked figures with the same figsize
        # align pixel-for-pixel on the y-axis regardless of tick-label width.
        fig.subplots_adjust(**fixed_margins)
        for ext in ("pdf", "png"):
            path = os.path.join(OUT, f"{filename}{out_suffix}.{ext}")
            plt.savefig(path, dpi=150)
            print(f"  Saved {filename}{out_suffix}.{ext}")
    else:
        plt.tight_layout(pad=0.8)
        for ext in ("pdf", "png"):
            path = os.path.join(OUT, f"{filename}{out_suffix}.{ext}")
            plt.savefig(path, dpi=150, bbox_inches="tight")
            print(f"  Saved {filename}{out_suffix}.{ext}")
    plt.close()


def fmt_rows(n):
    """Format row count as e.g. '238.4K' or '1.6M'."""
    if n >= 1_000_000:
        v = n / 1_000_000
        return f"{v:.1f}M" if v != int(v) else f"{int(v)}M"
    if n >= 1_000:
        v = n / 1_000
        s = f"{v:.1f}K"
        return s.replace(".0K", "K")
    return str(n)


def plot_test_case_composition():
    """Stacked horizontal bar chart — input composition by test case.

    T2–T7: designed input composition (exact target row counts per category).
    T1: runtime-measured composition averaged across 3 benchmark runs
        (created_trackers / existing_trackers / failed_rows from result JSONs),
        labelled with the input file row total (238.4K).
    """
    cases = ["T1", "T2", "T3", "T4", "T5", "T6", "T7"]
    data = {
        # T1: runtime averages across 3 E1 runs (real-world data, no synthetic markers)
        "T1": dict(new=219340, existing=7934, failed=48,  label_total=238433),
        # T2–T7: exact designed/input composition; label_total == new+existing+failed
        "T2": dict(new=16000,   existing=128000,  failed=16000,  label_total=160000),
        "T3": dict(new=128000,  existing=16000,   failed=16000,  label_total=160000),
        "T4": dict(new=160000,  existing=1280000, failed=160000, label_total=1600000),
        "T5": dict(new=1280000, existing=160000,  failed=160000, label_total=1600000),
        "T6": dict(new=21580,   existing=172640,  failed=21580,  label_total=215800),
        "T7": dict(new=172640,  existing=21580,   failed=21580,  label_total=215800),
    }

    # Colours: match the palette used throughout the paper figures
    C_NEW      = "#2cb5ae"
    C_EXISTING = "#1a3558"
    C_FAILED   = "#d97045"

    fig, ax = plt.subplots(figsize=(9.3, 3.7))

    for i, tc in enumerate(cases):
        d = data[tc]
        left = 0
        for val, color, label in [
            (d["new"],      C_NEW,      "New"),
            (d["existing"], C_EXISTING, "Existing"),
            (d["failed"],   C_FAILED,   "Failed"),
        ]:
            ax.barh(tc, val, left=left, color=color,
                    label=label if i == 0 else "_nolegend_",
                    height=0.62, zorder=3)
            left += val
        # Label shows input-file total (label_total); bar length = segment sum
        label_total = d.get("label_total", left)
        ax.text(label_total + label_total * 0.008, i, fmt_rows(label_total),
                va="center", ha="left", fontsize=13)

    # Vertical dashed gridlines
    ax.xaxis.grid(True, color="#dddddd", linestyle="--", linewidth=0.7, zorder=1)
    ax.set_axisbelow(True)

    ax.set_xlabel("Rows (K/M)")

    # X-axis tick formatter
    import matplotlib.ticker as ticker
    def row_fmt(x, _):
        if x >= 1_000_000: return f"{x/1_000_000:g}M"
        if x >= 1_000:     return f"{x/1_000:g}K"
        return str(int(x))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(row_fmt))

    # Extend x-axis right margin for labels
    max_total = max(d.get("label_total", d["new"]+d["existing"]+d["failed"])
                    for d in data.values())
    ax.set_xlim(0, max_total * 1.12)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend: show each category once, place inside bottom-right
    handles, labels = [], []
    seen = set()
    for h, l in zip(*ax.get_legend_handles_labels()):
        if l not in seen:
            handles.append(h); labels.append(l); seen.add(l)
    ax.legend(handles, labels, loc="lower right", fontsize=13,
              framealpha=0.9, ncol=3)

    plt.tight_layout(pad=0.8)
    for ext in ("pdf", "png"):
        path = os.path.join(OUT, f"test_cases_composition.{ext}")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        print(f"  Saved test_cases_composition.{ext}")
    plt.close()


def main():
    os.makedirs(OUT, exist_ok=True)
    data = load_data()

    plot(extract(data, "total_processing_time_sec_mean"),
         "Makespan (seconds)", "makespan_bidirectional")

    # Fig.4 reviewer fix: larger scale-group labels (Medium/Large/Skewed), shorter vertically
    plot(extract(data, "total_processing_time_sec_mean"),
         "Makespan (seconds)", "makespan_bidirectional",
         figsize=(9.4, 4.6), group_fontsize=17, out_suffix="_v2")

    plot(extract(data, "files_per_sec_mean"),
         "Throughput (files / sec)", "throughput_files_per_sec")

    plot(extract(data, "records_per_sec_mean"),
         "Throughput (records / sec)", "throughput_rec_per_sec")

    plot(extract(data, "end_to_end_latency_p95_sec_mean"),
         "p95 latency (seconds)", "p95_bidirectional")

    plot_test_case_composition()

    print("Done.")


if __name__ == "__main__":
    main()
