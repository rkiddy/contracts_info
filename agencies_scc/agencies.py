
import config

import time
import traceback
from pprint import pprint

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from sqlalchemy import create_engine


cfg = config.cfg()

engine = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
conn = engine.connect()


def db_exec(eng, this_sql):
    # print(f"sql: {sql}")
    if this_sql.strip().startswith('select'):
        return [dict(r) for r in eng.execute(this_sql).fetchall()]
    else:
        return eng.execute(this_sql)


def browser():
    opts = Options()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.implicitly_wait(5)
    return driver


def max():
    sql = "select max(pk) as pk from agencies"
    max = db_exec(conn, sql)
    if max is None or len(max) == 0:
        return 0
    elif max[0]['pk'] is None:
        return 0
    else:
        return max[0]['pk']


def protect(s):
    return s.replace("'", "''")


if __name__ == '__main__':

    ff = browser()

    try:

        ff.get("https://home.sccgov.org/government/agencies-departments")
        time.sleep(2)

        articles = ff.find_elements(By.TAG_NAME, 'article')

        agencies = list()

        # scrape the info into an articles list.
        #
        for article in articles:
            # print(f"article: {article}")

            links = article.find_elements(By.TAG_NAME, 'a')
            name = links[0].text

            info = None

            for div in article.find_elements(By.TAG_NAME, 'div'):
                class_attr = div.get_attribute('class')
                if class_attr is not None and class_attr.startswith('coh-accordion-tabs-content'):
                    info = div.get_attribute('innerHTML')

            if name != 'Home':
                agencies.append({'name': name, 'info': info})

        skip = ['Household Hazardous Waste Program', 'District 5, Supervisor Joe Simitian']

        # save the agency data if needed.
        #
        for agency in agencies:

            #print(f"\nagency: {agency['name']}\n")
            #print(f"    {agency['info']}")

            if agency['name'] in skip:
                print(f"Skipping: {agency['name']}")
                continue

            sql = f"select * from agencies where name = '{protect(agency['name'])}'"
            rows = db_exec(conn, sql)

            try:
                if rows is None or len(rows) == 0:
                    pk = max() + 1
                    sql = f"""
                        insert into agencies values ({pk}, '{protect(agency['name'])}',
                            '{protect(agency['info'])}')
                    """
                    # print(f"sql: {sql}")
                    db_exec(conn, sql)
                    print(f"Added to db: {agency['name']}")

                elif rows[0]['description'] != agency['info']:
                    sql = f"update agencies set description = '{protect(agency['info'])}' where pk = {rows[0]['pk']}"
                    db_exec(conn, sql)
                    print(f"Updated info: {agency['name']}")

                else:
                    print(f"No change: {agency['name']}")
            except:
                traceback.print_exc()

    except:
        traceback.print_exc()
    finally:
        ff.quit()
