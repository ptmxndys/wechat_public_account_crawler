import os
import sys
import json
import logging
import base64
from datetime import datetime
from time import sleep
import pandas as pd
from random import uniform
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import requests
from io import StringIO
import re
from lxml import etree, html
import hashlib
import shutil


DIST_DIR = 'dist'


def grep_media():

    with open(os.path.join(sys.path[0], 'config.json'), encoding='utf-8') as json_data:
        conf = json.loads(json_data.read())

    for account_name, read_mode in conf['Accounts'].items():

        # Read raw file
        article_list_file = os.path.join(sys.path[0], account_name + conf['RawDBPrefix'] + '.pkl')
        if os.path.exists(article_list_file):
            article_list = pd.read_pickle(article_list_file)
        else:
            article_list = pd.DataFrame(columns=['timestamp', 'title', 'link'])

        # Load database pkl if we are in delta mode, or create a new dataframe if in full mode
        media_index_file = os.path.join(sys.path[0], account_name + conf['MediaIndexPrefix'] + '.pkl')
        if os.path.exists(media_index_file):
            media_index = pd.read_pickle(media_index_file)
        else:
            media_index = pd.DataFrame(columns=['html_file', 'html_file_idx', 'media_type', 'src', 'sha256'])

        for idx, row in article_list.iterrows():

            logging.info('working on ' + str(idx+1))

            # Skip file we already captured
            file_name = os.path.join(sys.path[0], account_name + '_' + str(idx) + '.html')
            if not os.path.exists(file_name):
                # TODO: use a table to save this
                logging.error(f'{file_name} exists in database but not found in local directory')

            # Replace img data-src with src
            else:

                # open HTML file
                with open(file_name, 'r', encoding='utf-8') as f:
                    page = f.read()

                # generate HTML tree
                parser = etree.HTMLParser(encoding="utf-8")
                tree = etree.parse(StringIO(page), parser=parser)

                # Get all images of delayed loading
                img_elems = tree.xpath(r'//img[@data-src]')

                for img_elem in img_elems:

                    img_url = img_elem.attrib['data-src']

                    # We have worked on this media before...
                    if media_index[media_index['src'] == img_url].index.size > 0:
                        continue

                    # Hash the media link
                    img_hash = hashlib.sha256(img_url.encode()).hexdigest()

                    # Download media
                    data_type = img_elem.attrib['data-type'] if 'data-type' in img_elem.keys() else ''
                    if len(data_type) >= 1:
                        new_media_file_name = os.path.join('media', img_hash + '.' + data_type)
                    else:
                        new_media_file_name = os.path.join('media', img_hash)

                    response = requests.get(img_url)
                    if response.status_code == 200:
                        with open(os.path.join(sys.path[0], new_media_file_name), 'wb') as f:
                            f.write(response.content)

                    # Replace media link
                    # This does not work: xml parser will replace all & to &amp; (but we have hard-coded scripts)
                    # img_elem.attrib['src'] = new_media_file_name
                    # img_elem.attrib.pop('data-src', None)

                    # Save this into pickle
                    media_index.loc[media_index.index.size] = \
                        [account_name + '_' + str(idx) + '.html', idx, data_type, img_url, img_hash]

                    # I am not a robot.
                    sleep(uniform(0, 1))

                # Save media index to pickle
                media_index.to_pickle(media_index_file)

                # Filter media index of this file only
                media_of_html = media_index[media_index['html_file'] == account_name + '_' + str(idx) + '.html']
                # Replace all online media with local link, and replace delayed loading 'data-src' with 'src'
                for i, r in media_of_html.iterrows():
                    new_link = os.path.join('media', r['sha256'] + '.' + r['media_type'])
                    page = page.replace(r['src'], new_link)
                page = page.replace('data-src', 'src')

                # Write web content to file
                with open(os.path.join(sys.path[0], account_name + '_' + str(idx) + '_parsed.html'),
                          'w', encoding='utf-8') as f:
                    f.write(page)
                    f.close()


def grep_content():

    with open(os.path.join(sys.path[0], 'config.json'), encoding='utf-8') as json_data:
        conf = json.loads(json_data.read())

    for account_name, read_mode in conf['Accounts'].items():

        # Read raw file
        article_list_file = os.path.join(sys.path[0], account_name + conf['RawDBPrefix'] + '.pkl')
        if os.path.exists(article_list_file):
            article_list = pd.read_pickle(article_list_file)
        else:
            article_list = pd.DataFrame(columns=['timestamp', 'title', 'link'])

        for idx, row in article_list.iterrows():

            # Skip file we already captured
            if os.path.exists(os.path.join(sys.path[0], account_name + '_' + str(idx) + '.html')):
                continue

            # Request web content
            web_content = requests.get(row['link']).text

            # Write web content to file
            with open(os.path.join(sys.path[0], account_name + '_' + str(idx) + '.html'),
                      'w', encoding='utf-8') as f:
                f.write(web_content)
                f.close()

                # I am not a robot, I need some time to read this article...
                sleep(uniform(5, 10))


def get_summary(account_name):
    """
    Read HTML to extract author and publish date
    :param account_name:
    :return:
    """

    with open(os.path.join(sys.path[0], 'config.json'), encoding='utf-8') as json_data:
        conf = json.loads(json_data.read())

    article_list = pd.read_pickle(os.path.join(sys.path[0], account_name + conf['RawDBPrefix'] + '.pkl'))

    authors = []
    publish_time = []
    link = []

    for idx, row in article_list.iterrows():

        # open HTML file
        file_name = account_name + '_' + str(idx) + '_parsed.html'
        with open(file_name, 'r',  encoding='utf-8') as f:
            page = f.read()

        # generate HTML tree
        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(page), parser=parser)

        # Get author from mess
        author = tree.xpath(r'//html/body/div/div/div/div/div/div/span[@class="rich_media_meta rich_media_meta_text"]/text()')

        # Remove \n and space
        if len(author) > 0:
            author = author[0].replace(' ', '').replace('\n', '')

        # If the author has a wechat public account link, the DOM structure on HTML will be different:
        if len(author) == 0:
            author = tree.xpath('//html/body/div/div/div/div/div/div[@id="meta_content"][1]/span/span/text()')

            # Sometimes we just don't have an author
            if len(author) == 0:
                author = ''
            else:
                author = author[0].replace(' ', '').replace('\n', '')

        authors.append(author)

        # Get publish time, this is easier - we just grep 'publish_time' variable with regex.
        # pattern = re.compile(r'(?<=<em id=\"publish_time\" class=\"rich_media_meta rich_media_meta_text\">)[0-9]{4}-[0-9]{2}-[0-9]{2}(?=<\/em>)')
        pattern = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
        timestamp = pattern.findall(page)[0] if pattern.findall(page) else ''

        publish_time.append(timestamp)

        link.append(file_name)

    # Generate summary table
    article_list['author'] = authors
    article_list['publish_time'] = [datetime.strptime(x, '%Y-%m-%d') if x else datetime(1900, 1, 1) for x in publish_time]
    article_list['link'] = link

    writer = pd.ExcelWriter(os.path.join(sys.path[0], account_name + '_summary.xlsx'))
    article_list.to_excel(writer, 'Sheet1')
    writer.save()

    article_list.to_pickle(os.path.join(sys.path[0], account_name + '_summary.pkl'))

    html_tbl = """
        <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr style="text-align: left;">
              <th style="white-space:nowrap;">Date</th>
              <th>Post</th>
            </tr>
          </thead>
          <tbody>
        """
    for row in article_list.iterrows():
        shutil.copyfile(row[1]['link'], os.path.join(DIST_DIR, row[1]['link']))

        html_tbl += '<tr>\n' + \
                    f"<td>{row[1]['publish_time'].strftime('%Y-%m-%d')}</td>\n" + \
                    '<td><a href="file://{}">{}</a></br></td>\n'.format(os.path.join(sys.path[0], DIST_DIR, row[1]['link']), row[1]['title']) + \
                    '</tr>\n'

    html_tbl += """
          </tbody>
          </table>
          </div>
        """

    html_head = """
        <head>
          <title>WeChat Backup</title>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">
          <script src="https://code.jquery.com/jquery-3.4.1.slim.min.js" integrity="sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n" crossorigin="anonymous"></script>
          <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js" integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo" crossorigin="anonymous"></script>
          <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js" integrity="sha384-wfSDF2E50Y2D1uUdj0O3uMBJnjuUD4Ih7YwaYd1iqfktj0Uod8GCExl3Og8ifwB6" crossorigin="anonymous"></script>
          <style>
            td {
                font-size: 14px;
            }
          </style>
        </head>
        <body>

        <div class="container">
          <h2>WeChat Backup</h2>

        """

    html_trail = """
          </div>
          </body>
          </html>
        """

    html = html_head + html_tbl + html_trail
    f = open(f"{account_name}_summary.html", "w")
    f.write(html)
    f.close()


def get_article_links():

    # Read config
    with open(os.path.join(sys.path[0], 'config.json'), encoding='utf-8') as json_data:
        conf = json.loads(json_data.read())

    with open(os.path.join(sys.path[0], '.env'), encoding='utf-8') as json_data:
        env = json.loads(json_data.read())

    # Launch Selenium Firefox Driver
    executable_path = r'./geckodriver'
    driver = webdriver.Firefox(executable_path=executable_path)

    # Wechat main page
    driver.get('https://mp.weixin.qq.com/cgi-bin/loginpage?t=wxm2-login&lang=zh_CN')
    sleep(uniform(1, 2))

    # Login by username and password
    driver.find_elements_by_class_name('login__type__container__select-type')[1].click()
    sleep(uniform(1, 2))

    # Key-in username and password
    driver.find_element_by_name('account').send_keys(env['Username'])
    sleep(uniform(1, 2))

    pwd = base64.b64decode(env['Password']).decode('ascii')
    driver.find_element_by_name('password').send_keys(pwd)
    sleep(uniform(1, 2))

    driver.find_element_by_name('password').send_keys(Keys.RETURN)
    sleep(uniform(10, 12))

    # Click on '图文消息'
    btn = [x for x in driver.find_elements_by_class_name('new-creation__menu-title') if x.text == r'图文消息'][0]
    btn.click()
    sleep(uniform(2, 4))

    # Switch to new tab
    driver.switch_to.window(driver.window_handles[1])
    sleep(uniform(2, 4))

    # Find the 'add link' button on editor
    driver.find_element_by_id('edui91_body').click()
    sleep(uniform(2, 4))

    # Click '查找文章' radio button on modal
    radio = [x for x in driver.find_elements_by_class_name('weui-desktop-form__check-content') if x.text == r'查找公众号文章'][0]
    radio.click()
    sleep(uniform(2, 4))

    for account_name, read_mode in conf['Accounts'].items():

        # Key in account name to search
        input_box = driver.find_element_by_class_name('weui-desktop-form__input')
        input_box.send_keys(account_name)

        # Click on search glass icon
        btn = driver.find_elements_by_class_name('weui-desktop-form__input-append-in')[3]
        btn.click()
        sleep(uniform(1, 2))

        # Find account in dropdown list
        elm = [x for x in driver.find_elements_by_class_name('quote_account_nickname') if x.text == account_name][0]
        elm.click()
        sleep(uniform(10, 12))

        # Load database pkl if we are in delta mode, or create a new dataframe if in full mode
        if read_mode == 'full':
            df = pd.DataFrame(columns=['publish_date', 'title', 'link'])

        elif read_mode == 'delta':
            df = pd.read_pickle(os.path.join(sys.path[0], account_name + conf['RawDBPrefix'] + '.pkl'))

        else:
            raise ValueError('Read mode of account must be "full" or "delta"')

        delta_mode_break_flag = False

        # Get article links
        while True:
            # Find title and publish date of each article
            elms = zip(driver.find_elements_by_class_name('quote_article_title'), driver.find_elements_by_class_name('quote_article_date'))

            for elm, dt in elms:

                if read_mode == 'full':

                    # Add article to dataframe
                    df.loc[df.index.size] = [dt, elm.get_property('text'), elm.get_attribute('href')]

                elif read_mode == 'delta':

                    # Search if we have added all new articles
                    existing_entry = df[df['title'] == elm.get_property('text')]

                    if len(existing_entry.index) > 0:
                        delta_mode_break_flag = True
                        continue
                    else:
                        df.loc[df.index.size] = [dt, elm.get_property('text'), elm.get_attribute('href')]

            df.to_pickle(os.path.join(sys.path[0], account_name + conf['RawDBPrefix'] + '.pkl'))

            if delta_mode_break_flag:
                break

            next_btn = [x for x in driver.find_elements_by_class_name('weui-desktop-btn_mini') if x.text == r'下一页']

            # Exit if there's no more page to turn
            if len(next_btn) == 0:
                break

            else:
                # Turn to next page
                next_btn[0].click()

                # The critical sleep. Turning the page too fast will lead to account suspension.
                sleep(uniform(20, 30))

    driver.quit()


def main():
    get_article_links()
    grep_content()
    grep_media()
    get_summary(r'公众号')



if __name__ == "__main__":
    # execute only if run as a script
    main()

