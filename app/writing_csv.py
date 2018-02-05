from modules.database import SqlManipulator
import csv, os
from shutil import copyfile

DB_NAME = 'photos_data'
TABLE_NAME = 'instagram'
column = ['id', 'original_id', 'page_url', 'favorite', 'text', 'photo_url', 'timestamp']
search_conditions = []
conditions_file = ['./csv/sample_3rows.csv', './csv/sample_6rows.csv']

if __name__ == '__main__':
    sql_manipulator = SqlManipulator(DB_NAME)

    existed_records = sql_manipulator.select_all_records(TABLE_NAME)

    ## write all data to csv file
    with open('./csv/all.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column)
        writer.writerows(existed_records)

    ## extract csv info of prefectures
    for file in conditions_file:
        with open(file, newline='', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            for row in reader:
                print(row)
                search_conditions.append(row)

    ## ditribute photos into prefecture's parts
    for info in search_conditions:
        print(info[2])
        if os.path.isdir('./images/prefecture_photo/' + info[2]):
            None
        else:
            os.mkdir('./images/prefecture_photo/' + info[2])

        prefeture_records = sql_manipulator.select_like_records(TABLE_NAME, column[1], column[3], column[4], info)

        for record in prefeture_records:
            ## check is_belong -> insert 1
            sql_manipulator.update_record(TABLE_NAME, 'is_belong', 1, column[1], record[0], 1)

            src = "./images/master/" + str("{0:06d}".format(record[1])) + '_' + record[0] + '.jpg'
            dst = "./images/prefecture_photo/" + info[2] + "/" + str("{0:06d}".format(record[1])) + '_' + record[0] + '.jpg'
            copyfile(src, dst)

    ## distribute photos into indepentent part
    if os.path.isdir('./images/prefecture_photo/others'):
        None
    else:
        os.mkdir('./images/prefecture_photo/others')

    prefeture_records = sql_manipulator.select_not_records(TABLE_NAME, 'is_belong')
    print('others')

    for record in prefeture_records:
        src = "./images/master/" + str("{0:06d}".format(record[3])) + '_' + record[1] + '.jpg'
        dst = "./images/prefecture_photo/others" + "/" + str("{0:06d}".format(record[3])) + '_' + record[1] + '.jpg'
        copyfile(src, dst)
