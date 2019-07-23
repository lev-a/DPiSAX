# BestNeighbor

This tool to test a dataset sample for iSAX friendliness comes with the paper "BestNeighbor: Efficient Evaluation of kNN Queries on Large Time Series Databases by Choosing the Best Parallel Approach".


## Description 

BestNeighbor does a simple Fourier analysis of an input dataset of time series to evaluate the iSAX friendliness of the dataset by determining where most of the power is.

The main idea behind the tool is as follows. DPiSAX performs extremely well in terms of both quality and response time when the data resembles a random walk, with most of the energy in the low Fourier coefficients.
One might call that the iSAX-friendly regime. As the energy spreads to higher Fourier coefficients, iSAX pruning becomes less effective, leading to a higher time burden for exact iSAX and lower precision for approximate iSAX.
That  would  be  the iSAX-unfriendly regime.

BestNeighbor examines the input dataset to identify the range `[L..G]` that specifies the slice of Fourier coefficients that collectively account for at least 80% of the energy.
Low values of `G` , meaning that high frequencies do not have significant energy impact, are generally favorable for efficient iSAX pruning.
If there is substantial power at least up to the `30th` coefficient, then the dataset is in an iSAX-unfriendly regime, so using ParSketch instead would be recommended.
Otherwise, the tool outputs the recommended range of values for the number of segments that may lead to reasonable iSAX indexing and exact search times.

## Usage

`python BestNeighbor.py <filename>`

- `<filename>` contains the data sample to examine, one series per line. It must be in comma-separated text format, where the first column is the series identifier and the rest are data values, one per column.

### Prerequisites

- Python, version 2.7 or later
- numpy, version 1.16 or later
