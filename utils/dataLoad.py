from datetime import datetime
import pandas as pd
import os

def post_data_insert_function(cursor, connection, schema, tableName, payload, id_col):
    for post in payload:
        cursor.execute(f"SELECT {id_col} FROM r_posts WHERE {id_col} IN (%s)",(post[id_col],))
        exists = cursor.fetchone()
        
        if not exists:
            sql = f"INSERT INTO {schema}.{tableName} VALUES({'%s,'*len(post)+'%s'})"
            dataToInsert = {k: (lambda x: x)(v) for k, v in post.items()}
            dataToInsert['etl_insert_date'] = str(datetime.now())

            cursor.execute(sql, tuple(dataToInsert.values()))

        connection.commit()
    return 0

def loadData(path):

    df = pd.read_parquet(path)
    return df

def insertDataToMySQL(cursor, connection):
    df = loadData(f"{os.getenv('STAGING_AREA')}/new_posts.parquet")

    create_query = """
        CREATE TABLE IF NOT EXISTS r_posts (
            id VARCHAR(50) PRIMARY KEY,
            sub_reddit VARCHAR(100),
            post_type VARCHAR(10),
            title TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
            author VARCHAR(100),
            text_content TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,  
            url TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
            score BIGINT,
            num_comments INT,
            upvote_ratio DECIMAL(5, 2),
            over_18 VARCHAR(10),
            edited TIMESTAMP NULL DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            etl_insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    cursor.execute(create_query)
    id_col = df.columns[0] 
    post_data_insert_function(cursor=cursor,connection=connection, schema='reddit_db', tableName='r_posts', payload=df.to_dict('records'), id_col=id_col)




def insertDataToPostgreSQL(cursor, connection):
    df = loadData(f"{os.getenv('STAGING_AREA')}/transformed_data.parquet")

    create_query = """
            CREATE TABLE IF NOT EXISTS r_posts (
            post_id VARCHAR(50) PRIMARY KEY,
            title TEXT,
            text_content TEXT,
            sub_reddit VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            score BIGINT,
            sentiment_score DECIMAL(5,2),
            sentiment_score_flag VARCHAR(50),
            etl_insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    
    cursor.execute(create_query)

    id_col = df.columns[0] 
    post_data_insert_function(cursor=cursor,connection=connection, schema='public', tableName='r_posts', payload=df.to_dict('records'), id_col=id_col)

    return 0


def main():
    pass

if __name__ == "__main__":
    main()