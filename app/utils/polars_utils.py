"""
Utilities for working with Polars DataFrames.
This module contains functions for data manipulation using Polars.
"""

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("Warning: Polars is not available. Some functionality will be limited.")


def order_transcripts_by_expression(annotation_df, expression_df, expression_column="cpm_normalized_tmm", 
                             top_n=None, range_slice=None):
    """
    Orders transcripts from most to least expressed based on total expression across all samples.
    
    Args:
        annotation_df: Polars DataFrame with transcript annotation
        expression_df: Polars DataFrame with expression values
        expression_column: Column name containing expression values to sum
        top_n: Optional integer or list/tuple of two integers to return specific transcripts:
               - If integer, returns the top N most expressed transcripts
               - If [x, y], returns transcripts from position x to y (0-indexed)
               - If None, returns all transcripts
        range_slice: Deprecated, use top_n=[x, y] instead
        
    Returns:
        tuple: (ordered_expression_df, ordered_annotation_df)
    """
    if not POLARS_AVAILABLE:
        print("Warning: Polars not available, cannot order transcripts")
        return expression_df, annotation_df
        
    if expression_column not in expression_df.columns:
        print(f"Warning: {expression_column} not found in expression data. Using original transcript order.")
        return expression_df, annotation_df
        
    transcript_id_column = "transcript_id"
    
    # Verify transcripts in annotation exist in expression data
    annotation_transcripts = set(annotation_df[transcript_id_column].unique())
    expression_transcripts = set(expression_df[transcript_id_column].unique())
    
    # Find common transcripts
    common_transcripts = annotation_transcripts & expression_transcripts
    
    if len(common_transcripts) < len(annotation_transcripts):
        missing_count = len(annotation_transcripts) - len(common_transcripts)
        print(f"Warning: {missing_count} transcript(s) in annotation are missing from expression data")
    
    # Filter to keep only common transcripts
    filtered_annotation = annotation_df.filter(pl.col(transcript_id_column).is_in(common_transcripts))
    filtered_expression = expression_df.filter(pl.col(transcript_id_column).is_in(common_transcripts))
    
    # Aggregate expression data to compute total expression per transcript
    aggregated_df = filtered_expression.group_by(transcript_id_column).agg(
        pl.sum(expression_column).alias("total_expression")
    )
    
    # Sort by expression (descending) and then by transcript_id (alphabetically) to resolve ties
    sorted_transcripts = aggregated_df.sort(
        by=["total_expression", transcript_id_column], 
        descending=[True, False]
    )
    
    # Get ordered transcript IDs
    ordered_transcript_ids = sorted_transcripts[transcript_id_column].to_list()
    
    # Handle top_n selection
    if top_n is not None:
        if isinstance(top_n, (list, tuple)) and len(top_n) == 2:
            # Range selection [start, end]
            start, end = top_n
            if 0 <= start < end and end <= len(ordered_transcript_ids):
                transcripts_to_keep = ordered_transcript_ids[start:end]
            else:
                print(f"Warning: Invalid range {top_n}, using all transcripts")
                transcripts_to_keep = ordered_transcript_ids
        elif isinstance(top_n, int) and top_n > 0:
            # Top N selection
            if top_n < len(ordered_transcript_ids):
                transcripts_to_keep = ordered_transcript_ids[:top_n]
            else:
                transcripts_to_keep = ordered_transcript_ids
        else:
            print(f"Warning: Invalid top_n value {top_n}, using all transcripts")
            transcripts_to_keep = ordered_transcript_ids
    elif range_slice is not None:
        # Backward compatibility with range_slice
        start, end = range_slice
        if 0 <= start < end and end <= len(ordered_transcript_ids):
            transcripts_to_keep = ordered_transcript_ids[start:end]
        else:
            print(f"Warning: Invalid range_slice {range_slice}, using all transcripts")
            transcripts_to_keep = ordered_transcript_ids
    else:
        # Keep all transcripts
        transcripts_to_keep = ordered_transcript_ids
        
    # Join the total expression information for sorting
    filtered_annotation = filtered_annotation.join(
        sorted_transcripts.select([transcript_id_column, "total_expression"]),
        on=transcript_id_column,
        how="inner"
    )
    
    filtered_expression = filtered_expression.join(
        sorted_transcripts.select([transcript_id_column, "total_expression"]),
        on=transcript_id_column,
        how="inner"
    )
    
    # Filter to keep only selected transcripts and sort by total expression
    final_annotation = filtered_annotation.filter(
        pl.col(transcript_id_column).is_in(transcripts_to_keep)
    ).sort(
        by=["total_expression", transcript_id_column], 
        descending=[False, False]
    )
    
    final_expression = filtered_expression.filter(
        pl.col(transcript_id_column).is_in(transcripts_to_keep)
    ).sort(
        by=["total_expression", transcript_id_column], 
        descending=[False, False]
    )
    
    # Optionally drop the total_expression column if not needed
    if "total_expression" not in annotation_df.columns:
        final_annotation = final_annotation.drop("total_expression")
        
    if "total_expression" not in expression_df.columns:
        final_expression = final_expression.drop("total_expression")
    
    return final_expression, final_annotation


def aggregate_transcript_expression(expression_df, group_by="transcript_id", 
                                   value_column="cpm_normalized_tmm"):
    """
    Aggregates expression values for a given grouping.
    
    Args:
        expression_df: Polars DataFrame with expression values
        group_by: Column to group by (typically 'transcript_id' or 'gene_id')
        value_column: Expression column to aggregate
        
    Returns:
        Polars DataFrame with aggregated values
    """
    if not POLARS_AVAILABLE:
        print("Warning: Polars not available, cannot aggregate data")
        return None
        
    if value_column not in expression_df.columns:
        print(f"Warning: {value_column} not found in expression data")
        return None
        
    if group_by not in expression_df.columns:
        print(f"Warning: {group_by} not found in expression data")
        return None
    
    return expression_df.group_by(group_by).agg(
        pl.sum(value_column).alias(f"sum_{value_column}"),
        pl.mean(value_column).alias(f"mean_{value_column}"),
        pl.median(value_column).alias(f"median_{value_column}"),
        pl.min(value_column).alias(f"min_{value_column}"),
        pl.max(value_column).alias(f"max_{value_column}"),
        pl.count(value_column).alias("sample_count")
    )


def filter_expression_by_threshold(expression_df, column="cpm_normalized_tmm", threshold=1.0):
    """
    Filters expression data to only include values above a threshold.
    
    Args:
        expression_df: Polars DataFrame with expression values
        column: Expression column to filter on
        threshold: Minimum value to include
        
    Returns:
        Filtered Polars DataFrame
    """
    if not POLARS_AVAILABLE:
        print("Warning: Polars not available, cannot filter data")
        return expression_df
        
    if column not in expression_df.columns:
        print(f"Warning: {column} not found in expression data")
        return expression_df
        
    return expression_df.filter(pl.col(column) >= threshold)


def pivot_expression_data(expression_df, index_col="transcript_id", 
                         value_col="cpm_normalized_tmm", 
                         pivot_col="sample_id"):
    """
    Creates a pivot table from expression data.
    
    Args:
        expression_df: Polars DataFrame with expression values
        index_col: Column to use as index (rows)
        value_col: Expression values to pivot
        pivot_col: Column to use for pivoting (becomes columns)
        
    Returns:
        Pivoted Polars DataFrame with samples as columns
    """
    if not POLARS_AVAILABLE:
        print("Warning: Polars not available, cannot pivot data")
        return None
        
    required_cols = [index_col, value_col, pivot_col]
    for col in required_cols:
        if col not in expression_df.columns:
            print(f"Warning: {col} not found in expression data")
            return None
    
    # Pivot the data
    return expression_df.pivot(
        index=index_col,
        columns=pivot_col,
        values=value_col,
        aggregate_function="first"  # Each combination should be unique
    ) 