# Poker End-to-End Project on IRC Dataset

This project utilizes the IRC Poker dataset to analyze Poker hands from Holdem3 game files. It includes steps for data extraction, preparation, analysis, and building a predictive model.

## Table of Contents

1. [Setup](#setup)
2. [Project Workflow](#project-workflow)
   - [Data Extraction](#data-extraction)
   - [Data Preparation](#data-preparation)
   - [Data Analysis](#data-analysis)
   - [Model Building](#model-building)
3. [Dependencies](#dependencies)

## Setup

1. **Create a Virtual Environment**  
   Create a Python virtual environment to isolate project dependencies:
   ```bash
   python -m venv venv
   ```
2. **Activate the Virtual Environment**
   Activate the virtual environment and install all the dependencies
   ```bash
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Run the Data Extractor**
   Execute the `extractor.py` script to extract Poker hands data. After execution, a `hands.json` file will be created in the main directory:
   ```bash
   python extractor.py
   ```

## Project Workflow

### Data Extraction

The `extractor.py` script processes the Holdem3 game files and extracts Poker hands data, saving it in a `hands.json` file in the root directory.

### Data Preparation

The `data_preparation.ipynb` notebook contains all necessary steps to convert the `hands.json` file into a `.csv` format. The key output is the `data/holdem3.csv` file, which will be used in subsequent steps.

### Data Analysis

The `data_analysis.ipynb` notebook provides a detailed analysis of the participating players in Holdem3 game hands. Insights and patterns observed during the analysis are highlighted.

### Model Building

The `model_building.ipynb` notebook covers feature engineering and the development of a `LightGBM` model. This model predicts whether a player will win during the turn stage of the game.

### Dependencies

Ensure all the following dependencies are installed by using the requirements.txt file:

```bash
source venv/bin/activate
pip install -r requirements.txt

```
