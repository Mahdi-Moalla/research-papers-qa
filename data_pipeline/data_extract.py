import os
import os.path as osp
import re

import requests
from tqdm import tqdm
import fire
import requests


from IPython import embed as idbg


def process_markdown(markdown_url,
                     output_path,
                     tag):

    os.makedirs(output_path, exist_ok=True)

    response = requests.get(url=markdown_url)

    lines = response.text.split('\n')
    lines = [ line for line in lines if "https://arxiv.org/abs/" in line]
    #print(lines)

    for line in tqdm(lines):
        result = re.search(r'\(.*\)',line)
        link = result.group(0)[1:-1]
        link = link.replace("abs","pdf")
        file_id = link.split('/')[-1]
        link_response = requests.get(url=link)
        with open(osp.join(output_path,
                           f"{file_id}.pdf"), 'wb') as f:
            f.write(link_response.content)
        #idbg(colors='Linux')

if __name__=='__main__':
    fire.Fire()
