# Predicting Long-Term Contributors in Open Source Projects

Open source software (OSS) is essential to modern development but presents challenges, particularly around sustaining long-term contributor (LTC) engagement. This project replicates and expands on the study by Bao et al. (2019), using data mining, machine learning, and sentiment analysis to predict which contributors are likely to become LTCs on GitHub.

## Goals

This project addresses three key research questions:

- **RQ1**: How effectively can various machine learning models predict whether a developer will become an LTC?
- **RQ2**: What key features influence the retention of LTCs on GitHub?
- **RQ3**: How does sentiment in project discussions affect the likelihood of becoming an LTC?

## Key Findings

- **Early activity** is a strong predictor of long-term participation.
- **Random Forest** outperformed other models, with AUC scores of:
  - Year 1: 0.7705
  - Year 2: 0.8074
  - Year 3: 0.8137
- **Sentiment analysis** added limited value due to low comment engagement.
- **LTC recall remained low**, likely due to sparse early comment data.

These insights suggest OSS project managers can improve contributor retention by encouraging early, active participation and fostering collaborative communities.

## Data and Structure

The project uses a feature table dataset composed of:

1. Developer Monthly Activity  
2. Developer Profiles  
3. Repository Monthly Activity  
4. Repository Profiles  

### Folder Structure

- `src/`: Contains final, well-documented source code for data collection, transformation, and feature merging.
