import sqlite3
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import logging
from itertools import cycle

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_sentiment_graphs():
    try:
        # Connect to the database
        conn = sqlite3.connect('dataHUGE.db')
        cursor = conn.cursor()
        
        # Get all unique keywords
        cursor.execute("SELECT DISTINCT search_keyword FROM tweets UNION SELECT DISTINCT search_keyword FROM tiktoks")
        keywords = [row[0] for row in cursor.fetchall()]
        
        logging.info(f"Found {len(keywords)} unique keywords across both tables")
        logging.info(f"Generating sentiment graphs for {len(keywords)} keywords...")
        
        # Prepare data for combined graph
        combined_data_tweets = {}
        combined_data_tiktoks = {}
        colors = cycle(['b', 'g', 'r', 'c', 'm', 'y', 'k'])  # Cycle through colors for keywords
        
        for keyword in keywords:
            # Queries for individual graphs
            tweets_query = """
                SELECT created_at, AVG(sentiment_score_flair) as avg_sentiment
                FROM tweets
                WHERE search_keyword = ? AND sentiment_score_flair IS NOT NULL
                GROUP BY DATE(created_at)
                ORDER BY DATE(created_at)
            """
            tiktoks_query = """
                SELECT created_at, AVG(sentiment_score_flair) as avg_sentiment
                FROM tiktoks
                WHERE search_keyword = ? AND sentiment_score_flair IS NOT NULL
                GROUP BY DATE(created_at)
                ORDER BY DATE(created_at)
            """
            
            # Execute queries
            tweets_data = pd.read_sql_query(tweets_query, conn, params=(keyword,))
            tiktoks_data = pd.read_sql_query(tiktoks_query, conn, params=(keyword,))
            
            # Convert to datetime
            if not tweets_data.empty:
                tweets_data['created_at'] = pd.to_datetime(tweets_data['created_at'])
            if not tiktoks_data.empty:
                tiktoks_data['created_at'] = pd.to_datetime(tiktoks_data['created_at'])
            
            # Log data points
            logging.info(f"Keyword '{keyword}': {len(tweets_data)} tweet data points, {len(tiktoks_data)} TikTok data points")
            
            # Store data for combined graph
            color = next(colors)
            combined_data_tweets[keyword] = (tweets_data, color)
            combined_data_tiktoks[keyword] = (tiktoks_data, color)
            
            # Generate individual graph
            plt.figure(figsize=(12, 6))
            
            if not tweets_data.empty:
                plt.plot(tweets_data['created_at'], tweets_data['avg_sentiment'], 
                         'b-', marker='o', label='Twitter')
            
            if not tiktoks_data.empty:
                plt.plot(tiktoks_data['created_at'], tiktoks_data['avg_sentiment'], 
                         'r-', marker='s', label='TikTok')
            
            plt.axhline(y=0, color='k', linestyle='-', alpha=0.3)
            plt.title(f'Sentiment Analysis Over Time: {keyword}')
            plt.xlabel('Date Collected')
            plt.ylabel('Average Sentiment Score')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Adjust date formatting
            plt.gcf().autofmt_xdate()
            
            # Save individual graph
            output_dir = 'sentiment_graphs'
            os.makedirs(output_dir, exist_ok=True)
            plt.savefig(os.path.join(output_dir, f'sentiment_{keyword.replace(" ", "_")}.png'))
            plt.close()
            
            logging.info(f"Generated individual sentiment graph for '{keyword}'")
        
        # Generate combined graph
        plt.figure(figsize=(14, 8))
        
        for keyword, (tweets_data, color) in combined_data_tweets.items():
            if not tweets_data.empty:
                plt.plot(tweets_data['created_at'], tweets_data['avg_sentiment'], 
                         color=color, linestyle='-', marker='o', 
                         label=f'{keyword} (Twitter)')
        
        for keyword, (tiktoks_data, color) in combined_data_tiktoks.items():
            if not tiktoks_data.empty:
                plt.plot(tiktoks_data['created_at'], tiktoks_data['avg_sentiment'], 
                         color=color, linestyle='--', marker='s', 
                         label=f'{keyword} (TikTok)')
        
        plt.axhline(y=0, color='k', linestyle='-', alpha=0.3)
        plt.title('Combined Sentiment Analysis for All Keywords')
        plt.xlabel('Date Collected')
        plt.ylabel('Average Sentiment Score')
        plt.grid(True, alpha=0.3)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Adjust date formatting
        plt.gcf().autofmt_xdate()
        
        # Save combined graph
        plt.savefig(os.path.join(output_dir, 'sentiment_combined.png'), bbox_inches='tight')
        plt.close()
        
        logging.info("Generated combined sentiment graph for all keywords")
        
        conn.close()
        logging.info("All sentiment graphs generated successfully!")
        
    except Exception as e:
        logging.error(f"Error generating sentiment graphs: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_sentiment_graphs()