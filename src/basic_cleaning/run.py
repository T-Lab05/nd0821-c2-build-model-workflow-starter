#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Loading an input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    logger.info("Drop outliers on 'Price' column")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Drop sample outside of boundaries
    logger.info("Drop sample outside of boundaries")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting 'last_review' column into datetime type")
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    # Save the cleaned data on W&B
    logger.info("Saving the cleaned data on W&B")
    csv_name = "clean_sample.csv" 
    df.to_csv(csv_name, index=False)
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Artifact of data to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Artifact of cleaned data ",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Artifact type of output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price",
        required=True
    )


    args = parser.parse_args()

    go(args)
