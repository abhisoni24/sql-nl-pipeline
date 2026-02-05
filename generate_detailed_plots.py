
"""
Script for generating detailed plots and tables from evaluated experiment results.
Usage: python generate_detailed_plots.py <evaluated_results.jsonl> <output_dir>
"""
import sys
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

REPORT_CONTENT = []

def add_to_report(title, df, description=None):
    """Format DataFrame as markdown table and append to report."""
    global REPORT_CONTENT
    REPORT_CONTENT.append(f"## {title}")
    if description:
        REPORT_CONTENT.append(description)
    REPORT_CONTENT.append("")
    REPORT_CONTENT.append(df.to_markdown(index=False, floatfmt=".2f"))
    REPORT_CONTENT.append("")

def load_data(filepath):
    """Load evaluated results from JSONL."""
    print(f"Loading data from {filepath}...")
    records = []
    with open(filepath, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return pd.DataFrame(records)

def plot_vanilla_complexity_accuracy(df, output_dir):
    """
    1. Vanilla Prompt Accuracy by Complexity and Model
    """
    print("Generating Plot 1: Vanilla Complexity Accuracy...")
    
    vanilla_df = df[df['perturbation_source'] == 'baseline']
    
    if vanilla_df.empty:
        print("⚠️ No baseline/vanilla data found for Plot 1.")
        return

    # Group by model and complexity
    grouped = vanilla_df.groupby(['model_name', 'complexity'])['is_equivalent'].mean().reset_index()
    grouped['accuracy'] = grouped['is_equivalent'] * 100
    
    # Plot
    plt.figure(figsize=(12, 6))
    sns.barplot(data=grouped, x='complexity', y='accuracy', hue='model_name')
    plt.title('Vanilla Prompt Accuracy by Complexity and Model')
    plt.ylabel('Accuracy (%)')
    plt.xlabel('Complexity Type')
    plt.legend(title='Model', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/1_vanilla_complexity_accuracy.png')
    plt.close()
    
    # Table: Pivot to show Model x Complexity Grid
    pivot_table = grouped.pivot(index='complexity', columns='model_name', values='accuracy').reset_index()
    add_to_report("Vanilla Complexity Accuracy", pivot_table, "Accuracy (%) of Baseline (Vanilla) prompts by complexity.")

def plot_perturbation_category_accuracy(df, output_dir):
    """
    2. Perturbation Category Accuracy (Systematic vs LLM)
    """
    print("Generating Plot 2: Perturbation Category Accuracy...")
    
    pert_df = df[df['perturbation_source'].isin(['systematic', 'llm'])]
    
    if pert_df.empty:
        print("⚠️ No perturbation data found for Plot 2.")
        return

    # Accuracy per model, source, type
    grouped = pert_df.groupby(['model_name', 'perturbation_source', 'perturbation_type'])['is_equivalent'].mean().reset_index()
    grouped['accuracy'] = grouped['is_equivalent'] * 100
    
    # Plot
    g = sns.catplot(
        data=grouped, 
        x='perturbation_type', 
        y='accuracy', 
        hue='model_name', 
        row='perturbation_source',
        kind='bar',
        height=5, 
        aspect=2.5
    )
    g.set_xticklabels(rotation=45, ha='right')
    g.fig.suptitle('Accuracy by Perturbation Type (Systematic vs LLM)', y=1.02)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/2_perturbation_category_accuracy.png')
    plt.close()
    
    # Table: Flattened for detailed view
    # Columns: Perturbation Type | Source | Model A | Model B ...
    pivot_table = grouped.pivot_table(index=['perturbation_type', 'perturbation_source'], columns='model_name', values='accuracy').reset_index()
    add_to_report("Perturbation Category Accuracy", pivot_table, "Accuracy (%) by Perturbation Category and Source.")

def plot_compound_vs_vanilla(df, output_dir):
    """
    3. Compound vs Vanilla Accuracy
    """
    print("Generating Plot 3: Compound vs Vanilla Accuracy...")
    
    vanilla_df = df[df['perturbation_source'] == 'baseline']
    compound_df = df[df['perturbation_type'].str.contains('mixed', case=False) | df['perturbation_type'].str.contains('compound', case=False)]
    
    if vanilla_df.empty or compound_df.empty:
        print("⚠️ No distinct vanilla/compound data found for Plot 3.")
        return

    # Aggregate by Model
    vanilla_acc = vanilla_df.groupby('model_name')['is_equivalent'].mean() * 100
    compound_acc = compound_df.groupby('model_name')['is_equivalent'].mean() * 100
    
    comparison = pd.DataFrame({
        'Vanilla Accuracy': vanilla_acc,
        'Compound Accuracy': compound_acc
    }).reset_index()
    
    # Plot
    melted = comparison.melt('model_name', var_name='Prompt Type', value_name='Accuracy')
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted, x='model_name', y='Accuracy', hue='Prompt Type')
    plt.title('Vanilla vs Compound Perturbation Accuracy')
    plt.ylabel('Accuracy (%)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/3_compound_vs_vanilla_accuracy.png')
    plt.close()
    
    # Table (Calculate Drop)
    comparison['Performance Drop'] = comparison['Vanilla Accuracy'] - comparison['Compound Accuracy']
    add_to_report("Compound vs Vanilla Performance", comparison, "Comparison of accuracy between Baseline and Compound/Mixed perturbations.")

def plot_llm_per_category_pairplot(df, output_dir):
    """
    4. Systematic vs LLM Scatter & Table
    """
    print("Generating Plot 4: Systematic vs LLM Accuracy Comparison...")
    
    target_df = df[df['perturbation_source'].isin(['systematic', 'llm'])]
    
    acc_df = target_df.groupby(['model_name', 'perturbation_source', 'perturbation_type'])['is_equivalent'].mean().reset_index()
    acc_df['accuracy'] = acc_df['is_equivalent'] * 100
    
    pivoted = acc_df.pivot_table(
        index=['model_name', 'perturbation_type'], 
        columns='perturbation_source', 
        values='accuracy'
    ).reset_index()
    
    if 'systematic' not in pivoted.columns or 'llm' not in pivoted.columns:
        return

    plot_data = pivoted.dropna(subset=['systematic', 'llm'])
    
    if plot_data.empty:
        return

    # Plot Setup
    models = plot_data['model_name'].unique()
    cols = 3
    rows = (len(models) + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 5 * rows))
    axes = axes.flatten()
    
    for i, model in enumerate(models):
        ax = axes[i]
        subset = plot_data[plot_data['model_name'] == model]
        sns.scatterplot(data=subset, x='systematic', y='llm', ax=ax, s=100)
        for _, row in subset.iterrows():
            ax.text(row['systematic'], row['llm'], row['perturbation_type'], fontsize=8, alpha=0.7)
        lims = [0, 100]
        ax.plot(lims, lims, 'r--', alpha=0.5)
        ax.set_title(f'{model}')
        ax.set_xlabel('Systematic Acc')
        ax.set_ylabel('LLM Acc')
        ax.grid(True, alpha=0.3)

    for j in range(i + 1, len(axes)):
        axes[j].axis('off')
        
    plt.suptitle('Systematic vs LLM Perturbation Accuracy by Category', y=1.02)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/4_systematic_vs_llm_scatter.png')
    plt.close()
    
    # Table
    plot_data['Delta (Sys - LLM)'] = plot_data['systematic'] - plot_data['llm']
    add_to_report("Systematic vs LLM Alignment", plot_data, "Correlation between Systematic and LLM perturbation accuracies for overlapping categories.")

def main():
    if len(sys.argv) < 3:
        print("Usage: python generate_detailed_plots.py <input_jsonl> <output_dir>")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    df = load_data(input_file)
    if df.empty:
        print("❌ Dataframe is empty.")
        return
        
    # Generate Plots and Tables
    REPORT_CONTENT.append("# Detailed Experiment Analysis Report")
    REPORT_CONTENT.append(f"**Source Data:** `{input_file}`\n")
    
    plot_vanilla_complexity_accuracy(df, output_dir)
    plot_perturbation_category_accuracy(df, output_dir)
    plot_compound_vs_vanilla(df, output_dir)
    plot_llm_per_category_pairplot(df, output_dir)
    
    # Save Report
    report_path = f'{output_dir}/detailed_report.md'
    with open(report_path, 'w') as f:
        f.write("\n".join(REPORT_CONTENT))
        
    print(f"✅ All plots saved to {output_dir}")
    print(f"📝 Report saved to {report_path}")

if __name__ == "__main__":
    main()
