#!/usr/bin/env python3

# Author::    Justin Flannery  (mailto:jjuftin@juftin.com)

"""
Testing File for API Connections to NFL
"""

import logging
from datetime import datetime
from json import loads
from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import ParseResult, urlencode, urlunparse

from pandas import concat, DataFrame, read_parquet
from requests import request, Response

logger = logging.getLogger(__name__)


def generate_url(netloc: str,
                 scheme: str = "https",
                 path: Union[str, List[str]] = None,
                 query: Union[str, dict] = None,
                 fragment: str = None) -> str:
    """
    Generate a URL with certain parameters

    Parameters
    ----------
    netloc: str
        Network location part (netloc.com)
    scheme: str
        URL scheme specifier (https://)
    path: Union[str, List[str]]
        Hierarchical path. Accepts string, or list of strings to join (/path/to/url/)
    query: Union[str, dict]
        Query component (?query=string)
    fragment: str
        Fragment identifier (#fragment)

    Returns
    -------
    url: str
    """
    # JOIN PATH FOR LIST
    if isinstance(path, (tuple, list)):
        path = "/".join(path)
    # GENERATE QUERY STRING PARAMETERS
    if isinstance(query, dict):
        query = urlencode(query)
    parse_arguments = dict(scheme=scheme,
                           netloc=netloc,
                           path=path,
                           params=None,
                           query=query,
                           fragment=fragment)
    non_none_arguments = {key: str(value or "") for key, value in parse_arguments.items()}
    url_components = ParseResult(**non_none_arguments)
    return urlunparse(url_components)


def make_call(method: str,
              url: Optional[str] = None,
              netloc: Optional[str] = None,
              params: Optional[dict] = None,
              data: Optional[Union[dict, list, None]] = None,
              headers: Optional[dict] = None,
              scheme: Optional[str] = "https",
              path: Optional[Union[str, List[str], None]] = None,
              query: Optional[Union[str, dict, None]] = None,
              fragment: Optional[str] = None,
              **kwargs) -> Response:
    """
    Make a call via HTTP / HTTPS. Additional **kwargs are passed to requests.request

    If passed, a `url` parameter overrides all standard URL parameters

    Parameters
    ----------
    method: str
        method for the new Request object: GET, OPTIONS, HEAD, POST, PUT, PATCH, or DELETE.
    url: str
        URL to Call (overrides all URL component parameters)
    params: dict
        (optional) Dictionary, list of tuples or bytes to send in the query string for the request
    data: Union[dict, list]
        (optional) Dictionary, list of tuples, bytes, or file-like object to send in the body of the request
    headers: dict
        (optional) Dictionary of HTTP Headers to send with the request
    netloc: str
        Network location part (netloc.com)
    scheme: str
        URL scheme specifier (https://)
    path: Union[str, List[str]]
        Hierarchical path. Accepts string, or list of strings to join (/path/to/url/)
    params: str
        Parameters for last path element (;params)
    query: Union[str, dict]
        Query component (?query=string)
    fragment: str
        Fragment identifier (#fragment)

    Returns
    -------
    response: Response
    """
    if url is None and netloc is None:
        raise ValueError("Must provide either url or netloc parameters to `make_call` function.")
    elif url is not None:
        url_to_call = url
    else:
        url_to_call = generate_url(scheme=scheme, netloc=netloc, path=path,
                                   query=query, fragment=fragment)

    response = request(method=method, url=url_to_call,
                       params=params, headers=headers,
                       data=data, **kwargs)
    return response


# from nfl_sdk.base import get_file_path_array, introduce_repository, prepare_cross_year_data

def introduce_repository(branch: str = "master"):
    """
    Get some basic information from the GitHub Data Repository

    Parameters
    ----------
    branch: str
        Github branch
    """
    branch_owner = "guga31bb"
    repository_name = "nflfastR-data"
    github_recommended_header = {"Accept": "application/vnd.github.v3+json"}
    repo_response = make_call(method="get", netloc="api.github.com",
                              path=["repos", branch_owner, repository_name, "branches", branch],
                              headers=github_recommended_header)
    assert repo_response.status_code == 200
    response_content = loads(repo_response.content)
    last_commit = datetime.strptime(
        response_content["commit"]["commit"]["author"]["date"],
        "%Y-%m-%dT%H:%M:%S%z")
    logger.info(f"Loading data from {branch_owner}/{repository_name}:{branch}")
    logger.info(f'Last commit {datetime.utcnow() - last_commit.replace(tzinfo=None)}')


def get_file_path_array(years: List[Union[int, str]], branch="master",
                        file_extensions: Union[str, List[str]] = ".parquet") -> List[str]:
    """
    Get a list of Parquet files to ingest from nflfastR

    Parameters
    ----------
    years: List[Union[int, str]]
        List of years
    branch: str
        Github branch

    Returns
    -------
    file_paths: List[str]
        List of parquet file paths
    """
    branch_owner = "guga31bb"
    repository_name = "nflfastR-data"
    github_recommended_header = {"Accept": "application/vnd.github.v3+json"}
    repo_response = make_call(method="get", netloc="api.github.com",
                              path=["repos", branch_owner, repository_name,
                                    "contents", "data"],
                              params={"ref": branch},
                              headers=github_recommended_header)
    assert repo_response.status_code == 200
    response_content = loads(repo_response.content)
    if isinstance(file_extensions, str):
        file_extensions = [file_extensions]
    filtered_files = list()
    for file_dict in response_content:
        file_path = Path(file_dict["name"])
        year_match = any(str(year) in str(file_path) for year in years)
        if file_path.suffixes == file_extensions and year_match is True:
            filtered_files.append(file_dict["download_url"])
    logger.info(
        f"{len(filtered_files)} years of data returned between "
        f"{min([int(year) for year in years])} and "
        f"{max([int(year) for year in years])}")
    return filtered_files


def prepare_cross_year_data(file_paths: List[str]) -> DataFrame:
    """
    Generate a Concatenated DataFrame from Repo files

    Parameters
    ----------
    file_paths: List[str]
        List of github file paths

    Returns
    -------
    concat_df: DataFrame
        Pandas DataFrame
    """
    list_of_df = list()
    for file_url in file_paths:
        df = read_parquet(file_url)
        logger.info(f"Loading {len(df)} rows: {file_url}")
        list_of_df.append(df)
        del df
    concat_df = concat(list_of_df, ignore_index=True)
    logger.info(f"Final dataset prepared, {len(concat_df)} rows")
    return concat_df


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s [%(levelname)8s]: %(message)s [%(name)s]",
                        handlers=[logging.StreamHandler()],
                        level=logging.INFO)

    start_year = 2010
    end_year = 2020
    branch = "master"

    introduce_repository(branch=branch)
    year_files = get_file_path_array(years=range(start_year, end_year + 1, 1),
                                     branch=branch)
    final_df = prepare_cross_year_data(file_paths=year_files)
