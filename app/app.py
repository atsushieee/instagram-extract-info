from modules.database import SqlManipulator
from modules.scraping import get_json_info
import os
import urllib.request
from time import sleep

FRONT = 'https://www.instagram.com/graphql/query/?query_hash=298b92c8d7cad703f7565aa892ede943&variables=%7B%22tag_name%22%3A%22unknownjapan%22%2C%22first%22%3A100%2C%22after%22%3A%22'
END_CURSOR = 'J0HWneriQAAAF0HWneV2QAAAFnIA'
REAR = '%22%7D'
BASE_URL = FRONT + END_CURSOR + REAR
is_updated = True

is_created_db = False
is_deleted_db = False
DB_NAME = 'photos_data'
TABLE_NAME = 'instagram'
column = ['original_id', 'page_url', 'favorite', 'text', 'photo_url', 'timestamp']
column_type = ['varchar(20)', 'text', 'int(11)', 'text', 'text', 'timestamp']

ROOT_PATH = '/Users/a14886/Desktop/master'


if __name__ == '__main__':
    sql_manipulator = SqlManipulator(DB_NAME)

    ## create novel table
    if is_deleted_db:
        sql_manipulator.delete_table(TABLE_NAME)
    if is_created_db:
        sql_manipulator.create_table(TABLE_NAME)
        for i in range(len(column)):
            sql_manipulator.add_column(TABLE_NAME, column[i], column_type[i])
        # sql_manipulator.delete_column(TABLE_NAME)
    inserted_num = 0
    no_update_counter = 0

    ## extract json data
    while is_updated:
        json_data = get_json_info(BASE_URL)
        END_CURSOR = json_data['data']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
        BASE_URL = FRONT + str(END_CURSOR) + REAR
        print('end_cursor: ' + str(END_CURSOR))
        is_updated = json_data['data']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']

        ## insert into table
        for i in range(len(json_data['data']['hashtag']['edge_hashtag_to_media']['edges'])):
            inserted_num += 1
            if inserted_num % 50 == 0:
                print(BASE_URL)
                print('インサートしたレコード数: ' + str(inserted_num))

            # define of basic variance
            original_id = json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['id']
            page_url = json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['shortcode']
            current_favo = json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['edge_liked_by']['count']
            photo_url = json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['display_url']
            caption = ''
            if len(json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['edge_media_to_caption']['edges']) != 0:
                caption = json_data['data']['hashtag']['edge_hashtag_to_media']['edges'][i]['node']['edge_media_to_caption']['edges'][0]['node']['text']
                caption = caption.replace('\n', ' ')
                caption = caption.replace('\'', ' ')
                caption = caption.replace('\"', ' ')
            else:
                caption = 'none'

            column_value = [original_id, page_url, current_favo, caption, photo_url]

            # check for duplication -> insert or update
            existed_records = sql_manipulator.select_records(TABLE_NAME, column[0], original_id)
            if len(existed_records) > 0:
                previous_favo = existed_records[0][3]
                if previous_favo != current_favo:
                    #format favo to 6digit
                    os.rename(ROOT_PATH + "/" + str("{0:06d}".format(previous_favo)) + "_" + original_id + ".jpg", ROOT_PATH + "/" + str("{0:06d}".format(current_favo)) + "_" + original_id + ".jpg")
                    sql_manipulator.update_record(TABLE_NAME, column[2], current_favo, column[0], original_id, 1)
                    no_update_counter = 0
                    print("update record!!")
                else:
                    no_update_counter += 1
                    print("no-update counter: " + str(no_update_counter))
            else:
                no_update_counter = 0
                sql_manipulator.insert_into_table(TABLE_NAME, column[0], original_id)
                #format favo to 6digit
                urllib.request.urlretrieve(photo_url, ROOT_PATH + "/" + str("{0:06d}".format(current_favo)) + "_" + original_id + ".jpg")
                sleep(1)

                ####### if文で処理を分岐させる（）
                for j in range(len(column_value)-1):
                    if column_type[j+1] == 'text':
                        sql_manipulator.update_record(TABLE_NAME, column[j+1], column_value[j+1], column[0], column_value[0])
                    if column_type[j+1] == 'int(11)':
                        sql_manipulator.update_record(TABLE_NAME, column[j+1], column_value[j+1], column[0], column_value[0], 1)
