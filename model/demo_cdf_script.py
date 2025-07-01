import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from ast import literal_eval # For safely converting stringified lists


def remove_stopwords(text):
    """Remove stopwords from a given text."""
    if isinstance(text, str):  # Check if the input is a string
        words = [word for word in text.split() if word.lower() not in stop_words]
        return " ".join(words)  # Join words back into a string
    return ""


def lemmatize_text(text):
    """Lemmatize the words in a given text."""
    if isinstance(text, str):  # Check if the input is a string
        words = [lemmatizer.lemmatize(word) for word in text.split()]
        return " ".join(words)  # Join lemmatized words back into a string
    return ""


def jaccard_similarity(set1, set2):
    """Calculate Jaccard similarity between two sets."""
    intersection = len(set1.intersection(set2))  # Size of intersection
    union = len(set1.union(set2))  # Size of union
    return intersection / union if union != 0 else 0  # Avoid division by zero


def safe_literal_eval(value):
    """Safely evaluate a string to its literal representation."""
    if isinstance(value, str):  # Check if it's a string
        try:
            # Try to evaluate the string as a literal (e.g., list, tuple, dict)
            return literal_eval(value)
        except (ValueError, SyntaxError):
            # If evaluation fails, return the string as a list by splitting on commas
            return value.split(',') if value else []
    elif isinstance(value, float) and np.isnan(value):  # Handle NaN values
        return []  # Return an empty list for NaN values
    elif value is None:  # Handle None explicitly
        return []  # Return an empty list for None
    else:
        return value  # Return the value as-is if it's already in the correct format
    

def combined_similarity(movie1_index, movie2_index, genre_weight=0.2, overview_weight=0.5, keyword_weight=0.3):
    """
    Combine similarity scores from genres, overview, and keywords with adjustable weights.
    """
    genre_sim = jaccard_similarity(set(movie_genres[movie1_index]), set(movie_genres[movie2_index]))  # Jaccard similarity for genres
    overview_sim = cosine_sim_overview[movie1_index, movie2_index]  # Cosine similarity for overviews
    keyword_sim = cosine_sim_keywords[movie1_index, movie2_index]  # Cosine similarity for keywords
    return (genre_weight * genre_sim) + (overview_weight * overview_sim) + (keyword_weight * keyword_sim)  # Weighted sum of similarities


def get_recommendations_by_title(movie_title, top_n=10):
    """
    Get top N recommendations for a given movie title based on combined similarity scores.
    Warns if the title appears more than once in the dataset.
    """
    # Check if the input is a string
    if not isinstance(movie_title, str):
        return "Error: Movie title must be a string."

    # Count how many times the title appears
    matching_indices = [i for i, title in enumerate(movie_titles) if title == movie_title]

    if not matching_indices:
        return f"Error: '{movie_title}' not found in the dataset."

    if len(matching_indices) > 1:
        print(f"⚠️ Warning: The title '{movie_title}' appears {len(matching_indices)} times. Using the first occurrence.")

    movie_index = matching_indices[0]

    # Calculate similarity for all other movies
    similarities = [
        (i, combined_similarity(movie_index, i))
        for i in range(len(movie_titles)) if i != movie_index
    ]
    similarities.sort(key=lambda x: x[1], reverse=True)

    print(f"\nRecommendations for '{movie_title}' (index: {movie_index}):")
    for i, (idx, sim) in enumerate(similarities[:top_n], 1):
        print(f"{i}. Movie: {movie_titles[idx]} (ID: {movie_ids[idx]}), Similarity: {sim:.4f}")


def get_recommendations_by_id(input_id, top_n=10):
    """
    Get top N recommendations for a given movie ID based on combined similarity scores.
    """
    try:
        # Try to convert the input to integer
        movie_id = int(input_id)
    except (ValueError, TypeError):
        return "Error: Movie ID must be an integer."

    try:
        movie_index = movie_ids.index(movie_id)
    except ValueError:
        return f"Error: Movie ID {movie_id} not found in the dataset."

    # Compute similarity scores
    similarities = [
        (i, combined_similarity(movie_index, i))
        for i in range(len(movie_ids)) if i != movie_index
    ]
    similarities.sort(key=lambda x: x[1], reverse=True)

    print(f"\nRecommendations for Movie ID {movie_id} ({movie_titles[movie_index]}):")
    for i, (idx, sim) in enumerate(similarities[:top_n], 1):
        print(f"{i}. Movie: {movie_titles[idx]} (ID: {movie_ids[idx]}), Similarity: {sim:.4f}")


if __name__ == "__main__":
    DATASET_PATH  = "D:/Github_ThisPC/TMDB_RecoFlow/airflow/data/cbf_movie.csv"
    df = pd.read_csv(DATASET_PATH)

    # Download necessary NLTK data (only need to do this once)
    try:
        stopwords.words('english')
    except LookupError:
        nltk.download('stopwords')
    try:
        WordNetLemmatizer().lemmatize('test')
    except LookupError:
        nltk.download('wordnet')

    # Download WordNet if not already available
    try:
        nltk.data.find('corpora/wordnet.zip')
    except LookupError:
        nltk.download('wordnet')

    
    # Preprocessing functions
    stop_words = set(stopwords.words('english'))  # Define stop words for text processing
    lemmatizer = WordNetLemmatizer()  # Initialize lemmatizer for stemming words to their root form

    # Apply the function to the specified columns
    features = ['keywords', 'genres']
    for feature in features:
        df[feature] = df[feature].apply(safe_literal_eval)

    # Assuming you already have 'df' as your DataFrame
    df['overview'] = df['overview'].fillna('')  # Replace NaN in overview
    df['keywords'] = df['keywords'].fillna('')  # Replace NaN in keywords

    # Feature Extraction
    # Create TF-IDF matrix for movie overviews
    tfidf_overview = TfidfVectorizer(stop_words='english')
    tfidf_overview_matrix = tfidf_overview.fit_transform(df['overview'])

    # Convert keywords into strings (if they are lists) and create a TF-IDF matrix for keywords
    # Ensure keywords are properly joined into a string for vectorization
    keywords_ = [' '.join(keywords) if isinstance(keywords, list) else keywords for keywords in df['keywords']]
    vector = CountVectorizer(stop_words='english')
    vector_keywords_matrix = vector.fit_transform(keywords_)

    # Extract genres and titles as lists for similarity computation
    movie_genres = df['genres'].tolist()
    movie_titles = df['title'].tolist()
    movie_ids = df['movie_id'].tolist()

    ### TODO Error MemoryError: Unable to allocate 29.1 GiB for an array with shape (3911458708,) and data type int64
    # 28 cosine_sim_overview = cosine_similarity(tfidf_overview_matrix)
    #  29 cosine_sim_keywords = cosine_similarity(vector_keywords_matrix)

    # Calculate cosine similarity for both overview and keywords
    cosine_sim_overview = cosine_similarity(tfidf_overview_matrix)
    cosine_sim_keywords = cosine_similarity(vector_keywords_matrix)

    # Example Usage
    recommendations = get_recommendations_by_id(49026)  # Get recommendations for 'The Dark Knight Rises'
    print(recommendations)

    recommendations = get_recommendations_by_id(99999)  # Test with a movie not in the dataset
    print(recommendations)

    recommendations = get_recommendations_by_id('Error')  # Test with a non-string input
    print(recommendations)

