import psycopg2
import sys
import datetime, time

schemaName = sys.argv[1]


def exec_sql(connection):
    fetch_table_list = "Select table_name from Information_Schema.table where schema_name = %s" % schemaName
    cursor = connection.cursor()
    cursor.execute(fetch_table_list)
    records = cursor.fetchall()
    transformed = []

    for i in records:
        new_record = str(i).replace('(', '').replace(')', '').replace('\'', '')
        transformed.append(new_record)

    table_excepted = ['']
    list_new = []

    for x in table_excepted:
        for y in transformed:
            if x in y:
                list_new.append(y)

    final_list = list(set(transformed) - set(list_new))

    final_dict = {}

    for table in final_list:
        duplicate_count = "select '%s, %s', count(*) from (Select primary_key, count(*) from %s.%s t group by 1 " \
                          "having count(*) > 1) sub;" % (schemaName, str(table, schemaName, str(table)))
        cursor.execute(duplicate_count)
        records_count = cursor.fetchall()
        final_dict.update(dict((x, y) for x, y in records_count))

        zeroless_dict = dict()
        for (k, v) in final_dict.items():
            if v > 1:
                zeroless_dict[k] = v
        for (k, v) in zeroless_dict.items():
            print(k)
        connection.commit()


def main():
    connection = psycopg2.connect(database='', user='', passwprd='', host='', port='')
    exec_sql(connection)
    connection.close()


if __name__ == "__main__":
    main()
