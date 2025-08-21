import os
import os.path as osp
import re

import requests
from tqdm import tqdm
import fire
import requests
from pprint import pprint


from IPython import embed as idbg


def arxiv_process_link(link: str):
    link=link.replace("abs","pdf").replace(".pdf","")
    return {"source":"arxiv",
            "source_id":link.split("/")[-1],
            "download_link":link}

def find_pdf_download_links(link: str):
    if "arxiv" in link:
        return arxiv_process_link(link)
    else:
        return None

def match_markdown_link(line: str):
    result=re.search(r'\[.*\]\(.*\)',line)
    if result is not None:
        markdown_link=result.group(0)
        link_start=markdown_link.rindex("(")+1
        return markdown_link[link_start:-1]
    else:
        return None

def process_markdown(markdown_url: str,
                     tag: str):

    response = requests.get(url=markdown_url)

    lines = response.text.split('\n')
    links = [match_markdown_link(line) for line in lines]
    links = [link for link in links if link is not None]
    links = [find_pdf_download_links(link) for link in links]
    links = [link for link in links if link is not None]
    pprint(links, indent=2)

    # for line in tqdm(lines):
    #     result = re.search(r'\(.*\)',line)
    #     link = result.group(0)[1:-1]
    #     link = link.replace("abs","pdf")
    #     file_id = link.split('/')[-1]
    #     link_response = requests.get(url=link)
    #     with open(osp.join(output_path,
    #                        f"{file_id}.pdf"), 'wb') as f:
    #         f.write(link_response.content)
    #idbg(colors='Linux')

if __name__=='__main__':
    fire.Fire()
