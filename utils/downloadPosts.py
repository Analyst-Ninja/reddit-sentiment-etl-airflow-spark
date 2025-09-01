from datetime import datetime
import pandas as pd
import os

def extractDataToParquet(subRedditList, redditInstance):
    new_posts_list = []
    for subreddit in subRedditList:
        subredditObj = redditInstance.subreddit(subreddit)

        new_posts = subredditObj.new(limit=10000)

        for post in new_posts:
            post_dict = {
                'id' : post.id,
                'sub_reddit' : subreddit,
                'post_type' : 'new',
                'title' : post.title,
                'author' : post.author.name if post.author else None,
                'text_content' : post.selftext if post.selftext else None,
                'url' : post.url,
                'score' : int(post.score),
                'num_comments' : post.num_comments,
                'upvote_ratio' : post.upvote_ratio,
                'over_18' : post.over_18,
                'edited' : datetime.fromtimestamp(post.edited).strftime('%Y-%m-%d %H:%M:%S') if post.edited != 0 else None ,
                'created_at' : datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                'fetched_at' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            new_posts_list.append(post_dict)
    new_posts_df = pd.DataFrame(new_posts_list)
    new_posts_df.to_parquet(f"{os.getenv('STAGING_AREA')}/new_posts.parquet")

    return 0
