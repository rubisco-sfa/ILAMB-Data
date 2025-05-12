import datetime
import os

import cftime as cf
import numpy as np
import requests
import xarray as xr
from intake_esgf import ESGFCatalog
from tqdm import tqdm


def download_from_html(remote_source: str, local_source: str | None = None) -> str:
    """
    Download a file from a remote URL to a local path.
    If the "content-length" header is missing, it falls back to a simple download.
    """
    if local_source is None:
        local_source = os.path.basename(remote_source)
    if os.path.isfile(local_source):
        return local_source

    resp = requests.get(remote_source, stream=True)
    try:
        total_size = int(resp.headers.get("content-length"))
    except (TypeError, ValueError):
        total_size = 0

    with open(local_source, "wb") as fdl:
        if total_size:
            with tqdm(
                total=total_size, unit="B", unit_scale=True, desc=local_source
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        fdl.write(chunk)
                        pbar.update(len(chunk))
        else:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
    return local_source


def download_from_zenodo(record: dict):
    """
    Download all files from a Zenodo record dict into a '_temp' directory.
    Example for getting a Zenodo record:

        # Specify the dataset title you are looking for
        dataset_title = "Global Fire Emissions Database (GFED5) Burned Area"

        # Build the query string to search by title
        params = {
            "q": f'title:"{dataset_title}"'
        }

        # Define the Zenodo API endpoint
        base_url = "https://zenodo.org/api/records"

        # Send the GET request
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print("Error during search:", response.status_code)
            exit(1)

        # Parse the JSON response
        data = response.json()

        # Get record dictionary
        records = data['hits']['hits']
        record = data['hits']['hits'][0]
    """
    download_dir = "_temp"
    os.makedirs(download_dir, exist_ok=True)

    title = record.get("metadata", {}).get("title", "No Title")
    pub_date = record.get("metadata", {}).get("publication_date", "No publication date")
    print(f"Found record:\n  Title: {title}\n  Publication Date: {pub_date}")

    for file_info in record.get("files", []):
        file_name = file_info.get("key")
        file_url = file_info.get("links", {}).get("self")
        local_file = os.path.join(download_dir, file_name)

        if file_url:
            print(f"Downloading {file_name} from {file_url} into {download_dir}...")
            download_from_html(file_url, local_source=local_file)
        else:
            print(f"File URL not found for file: {file_name}")


# I think this can be useful to make sure people export the netcdfs the same way every time
def get_filename(attrs: dict, time_range: str) -> str:
    """
    Generate a NetCDF filename using required attributes and a time range.

    Args:
        attrs (dict): Dictionary of global attributes.
        time_range (str): Time range string to embed in the filename.

    Returns:
        str: Formatted filename.

    Raises:
        ValueError: If any required attributes are missing from `attrs`.
    """
    required_keys = [
        "variable_id",
        "frequency",
        "source_id",
        "variant_label",
        "grid_label",
    ]

    missing = [key for key in required_keys if key not in attrs]
    if missing:
        raise ValueError(
            f"Missing required attributes: {', '.join(missing)}. "
            f"Expected keys: {', '.join(required_keys)}"
        )

    filename = "{variable_id}_{frequency}_{source_id}_{variant_label}_{grid_label}_{time_mark}.nc".format(
        **attrs, time_mark=time_range
    )
    return filename


def get_cmip6_variable_info(variable_id: str) -> dict[str, str]:
    """ """
    df = ESGFCatalog().variable_info(variable_id)
    return df.iloc[0].to_dict()


def set_time_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Ensure the xarray dataset's time attributes are formatted according to CF-Conventions.
    """
    assert "time" in ds
    da = ds["time"]

    # Ensure time is an accepted xarray time dtype
    if np.issubdtype(da.dtype, np.datetime64):
        ref_date = np.datetime_as_string(da.min().values, unit="s")
    elif isinstance(da.values[0], cf.datetime):
        ref_date = da.values[0].strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise TypeError(
            f"Unsupported xarray time format: {type(da.values[0])}. Accepted types are np.datetime64 or cftime.datetime."
        )

    da.encoding = {
        "units": f"days since {ref_date}",
        "calendar": da.encoding.get("calendar"),
    }
    da.attrs = {
        "axis": "T",
        "standard_name": "time",
        "long_name": "time",
    }
    ds["time"] = da
    return ds


def set_lat_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Ensure the xarray dataset's latitude attributes are formatted according to CF-Conventions.
    """
    assert "lat" in ds
    da = ds["lat"]
    da.attrs = {
        "axis": "Y",
        "units": "degrees_north",
        "standard_name": "latitude",
        "long_name": "latitude",
    }
    ds["lat"] = da
    return ds


def set_lon_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Ensure the xarray dataset's longitude attributes are formatted according to CF-Conventions.
    """
    assert "lon" in ds
    da = ds["lon"]
    da.attrs = {
        "axis": "X",
        "units": "degrees_east",
        "standard_name": "longitude",
        "long_name": "longitude",
    }
    ds["lon"] = da
    return ds


def set_var_attrs(
    ds: xr.Dataset, var: str, units: str, standard_name: str, long_name: str
) -> xr.Dataset:
    """
    Ensure the xarray dataset's variable attributes are formatted according to CF-Conventions.
    """
    assert var in ds
    da = ds[var]
    da.attrs = {"units": units, "standard_name": standard_name, "long_name": long_name}
    ds[var] = da
    return ds


def gen_utc_timestamp(time: float | None = None) -> str:
    if time is None:
        time = datetime.datetime.now(datetime.UTC)
    else:
        time = datetime.datetime.fromtimestamp(time)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def add_time_bounds_monthly(ds: xr.Dataset) -> xr.Dataset:
    """
    Add monthly time bounds to an xarray Dataset.

    For each timestamp in the dataset's 'time' coordinate, this function adds a new
    coordinate called 'time_bounds' with the first day of the month and the first
    day of the next month. These bounds follow CF conventions.

    Args:
        ds (xr.Dataset): Dataset with a 'time' coordinate of monthly timestamps.

    Returns:
        xr.Dataset: Modified dataset with a 'time_bounds' coordinate and updated
                    attributes on the 'time' coordinate.
    """

    def _ymd_tuple(da: xr.DataArray) -> tuple[int, int, int]:
        """Extract (year, month, day) from a single-element datetime DataArray."""
        if da.size != 1:
            raise ValueError("Expected a single-element datetime for conversion.")
        return int(da.dt.year), int(da.dt.month), int(da.dt.day)

    def _make_timestamp(t: xr.DataArray, ymd: tuple[int, int, int]) -> np.datetime64:
        """Construct a timestamp matching the type of the input time value."""
        try:
            return type(t.item())(*ymd)  # try using the same class as the input
        except Exception:
            # fallback to datetime64 if direct construction fails
            return np.datetime64(f"{ymd[0]:04d}-{ymd[1]:02d}-{ymd[2]:02d}")

    lower_bounds = []
    upper_bounds = []

    for t in ds["time"]:
        year, month, _ = _ymd_tuple(t)
        lower_bounds.append(_make_timestamp(t, (year, month, 1)))

        # First day of the next month (verbose-ified for easier readability)
        if month == 12:
            next_month = (year + 1, 1, 1)
        else:
            next_month = (year, month + 1, 1)
        upper_bounds.append(_make_timestamp(t, next_month))

    bounds_array = np.array([lower_bounds, upper_bounds]).T
    ds = ds.assign_coords(time_bounds=(("time", "bounds"), bounds_array))
    ds["time_bounds"].attrs["long_name"] = "time_bounds"
    ds["time"].attrs["bounds"] = "time_bounds"

    return ds


def add_time_bounds_single(
    ds: xr.Dataset, start_date: str, end_date: str
) -> xr.Dataset:
    """
    Add a single time coordinate with bounds to an xarray Dataset.

    The 'time' coordinate is set to the midpoint between the start and end dates,
    and a 'time_bounds' coordinate is added following CF conventions.

    Args:
        ds (xr.Dataset): Dataset to modify.
        start_date (str): Start of the time bounds (e.g., '2020-01-01').
        end_date (str): End of the time bounds (e.g., '2020-02-01').

    Returns:
        xr.Dataset: Dataset with a single 'time' coordinate and 'time_bounds'.
    """
    start = np.datetime64(start_date)
    end = np.datetime64(end_date)

    if end <= start:
        raise ValueError("end_date must be after start_date.")

    # Midpoint timestamp for 'time' coordinate
    midpoint = start + (end - start) / 2
    time = xr.DataArray([midpoint], dims="time", name="time")

    # Time bounds as a 2D array with shape (1, 2)
    time_bounds = xr.DataArray(
        np.array([[start, end]], dtype="datetime64[ns]"),
        dims=("time", "bounds"),
        name="time_bounds",
    )

    # Expand the dataset with this new time dimension and assign bounds
    ds = ds.expand_dims({"time": time})
    ds = ds.assign_coords(time_bounds=time_bounds)
    ds["time_bounds"].attrs["long_name"] = "time_bounds"
    ds["time"].attrs["bounds"] = "time_bounds"

    return ds


def set_cf_global_attributes(
    ds: xr.Dataset,
    *,  # keyword only for the following args
    title: str,
    institution: str,
    source: str,
    history: str,
    references: str,
    comment: str,
    conventions: str,
) -> xr.Dataset:
    """
    Set required NetCDF global attributes according to CF-Conventions 1.12.

    Args:
        ds (xr.Dataset): The xarray dataset to which global attributes will be added.
        title (str): Short description of the file contents.
        institution (str): Where the original data was produced.
        source (str): Method of production of the original data.
        history (str): List of applications that have modified the original data.
        references (str): References describing the data or methods used to produce it.
        comment (str): Miscellaneous information about the data or methods used.
        conventions (str): The name of the conventions followed by the dataset.

    Returns:
        xr.Dataset: The dataset with updated global attributes.

    Raises:
        ValueError: If a required global attribute is missing.
    """

    # Build and validate attributes
    attrs = {
        "title": title,
        "institution": institution,
        "source": source,
        "history": history,
        "references": references,
        "comment": comment,
        "Conventions": conventions,
    }

    # Ensure all values are explicitly set (None not allowed)
    missing = [k for k, v in attrs.items() if v is None]
    if missing:
        raise ValueError(f"Missing required global attributes: {', '.join(missing)}")

    ds.attrs.update(attrs)
    return ds
