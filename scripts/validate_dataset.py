"""
A script that checks an input dataset (netCDF file) for adherence to ILAMB standards.
The netCDF can contain site data or gridded data.
"""

import sys
from typing import Literal

import cftime
import numpy as np
import xarray as xr
from pydantic import BaseModel, ConfigDict, field_validator


def get_dim_name(
    dset: xr.Dataset | xr.DataArray,
    dim: Literal["time", "lat", "lon", "depth", "site"],
) -> str:
    dim_names = {
        "time": ["time"],
        "lat": ["lat", "latitude", "Latitude", "y", "lat_"],
        "lon": ["lon", "longitude", "Longitude", "x", "lon_"],
        "depth": ["depth"],
    }
    # Assumption: the 'site' dimension is what is left over after all others are removed
    if dim == "site":
        try:
            get_dim_name(dset, "lat")
            get_dim_name(dset, "lon")
            # raise NoSiteDimension("Dataset/dataarray is spatial")
        except KeyError:
            pass
        possible_names = list(
            set(dset.dims) - set([d for _, dims in dim_names.items() for d in dims])
        )
        if len(possible_names) == 1:
            return possible_names[0]
        msg = f"Ambiguity in locating a site dimension, found: {possible_names}"
        # raise NoSiteDimension(msg)
    possible_names = dim_names[dim]
    dim_name = set(dset.dims).intersection(possible_names)
    if len(dim_name) != 1:
        msg = f"{dim} dimension not found: {dset.dims} "
        msg += f"not in [{','.join(possible_names)}]"
        raise KeyError(msg)
    return str(dim_name.pop())


def is_spatial(da: xr.DataArray) -> bool:
    try:
        get_dim_name(da, "lat")
        get_dim_name(da, "lon")
        return True
    except KeyError:
        pass
    return False


# spatial validator
class ILAMBDataset(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    ds: xr.Dataset

    @field_validator("ds")
    @classmethod
    def check_vars(cls, ds: xr.Dataset) -> xr.Dataset:
        # Check that there are data variables
        if not ds.data_vars:
            raise ValueError(
                "Dataset does not have any data variables. An example data variable is 'cSoil'."
            )
        # Check that the dataset has at least one variable but not more than 2
        if len(ds.data_vars) >= 3:
            raise ValueError(
                f"Dataset has too many data variables {ds.data_vars}. The measurement and the uncertainty are the only expected data variables. There should be one netCDF file per data variable if a dataset has multiple data variables."
            )

    @field_validator("ds")
    @classmethod
    def global_attrs(cls, ds: xr.Dataset) -> xr.Dataset:
        # Check that the dataset has the required global attribute keys
        missing = set(
            [
                "title",
                "source_version_number",
                "institution",
                "source",
                "history",
                "references",
                "Conventions",
            ]
        ) - set(ds.attrs.keys())
        if missing:
            raise ValueError(
                f"Dataset does not properly encode global attributes, {missing=}"
            )
        return ds

    @field_validator("ds")
    @classmethod
    def time_dim(cls, ds: xr.Dataset) -> xr.Dataset:
        # Check that the dataset has a properly set-up time dimension
        dimensions = ds.dims
        time_dim_present = "time" in dimensions
        time_var = ds["time"]
        time_attrs = time_var.attrs

        # Check if the time dimension is present
        if not time_dim_present:
            raise ValueError(
                f"Dataset does not have a time dimension, {dimensions=}. Expected a dimension called 'time'."
            )

        # Check if time values are decoded as datetime objects
        time_dtype = type(time_var.values[0])
        if not (
            np.issubdtype(time_var.values.dtype, np.datetime64)
            or isinstance(time_var.values[0], cftime.datetime)
        ):
            raise TypeError(
                f"Time values are not properly decoded as datetime objects: {time_dtype=}"
            )

        # Check time attributes: axis, long_name, standard_name
        missing = set(["axis", "long_name", "standard_name"]) - set(time_attrs)
        if missing:
            raise ValueError(
                f"Dataset is missing time-specific attributes, {missing=}."
            )
        else:
            # Check the axis and standard_name to ensure they're correct; long_name can vary.
            correct_axis_name = time_attrs["axis"] == "T"
            if not correct_axis_name:
                raise TypeError(
                    f"The time dimension's axis attribute is {time_attrs['axis']}. Expected 'T'."
                )
            correct_std_name = time_attrs["standard_name"] == "time"
            if not correct_std_name:
                raise TypeError(
                    f"The time dimension's standard_name attribute is {time_attrs['standard_name']}. Expected 'time'."
                )

        # Check time units encoding and formatting
        time_encoding = time_var.encoding
        if "units" not in time_encoding or "since" not in time_encoding["units"]:
            raise ValueError(
                f"Time encoding is missing or incorrect, {time_encoding=}. Expected 'days since YYYY:MM:DD HH:MM'"
            )

        # Check time calendar encoding
        if "calendar" in time_encoding:
            valid_calendars = [
                "standard",
                "gregorian",
                "proleptic_gregorian",
                "noleap",
                "all_leap",
                "360_day",
                "julian",
            ]

            if time_encoding["calendar"] not in valid_calendars:
                # Check for explicitly defined calendar attributes
                if "month_lengths" in time_attrs:
                    # Validate month_lengths
                    month_lengths = time_attrs["month_lengths"]
                    if len(month_lengths) != 12 or not all(
                        isinstance(m, (int, np.integer)) for m in month_lengths
                    ):
                        raise ValueError(
                            "month_lengths must be a list of 12 integer values."
                        )

                    # Validate leap year settings if present
                    if "leap_year" in time_attrs:
                        leap_year = time_attrs["leap_year"]
                        if not isinstance(leap_year, (int, np.integer)):
                            raise ValueError("leap_year must be an integer.")

                        if "leap_month" in time_attrs:
                            leap_month = time_attrs["leap_month"]
                            if not (1 <= leap_month <= 12):
                                raise ValueError("leap_month must be between 1 and 12.")
                else:
                    raise ValueError(
                        f"Unrecognized calendar '{time_encoding['calendar']}' and no explicit month_lengths provided."
                    )
        else:
            raise ValueError("Calendar attribute is missing from the time encoding.")

        # Check bounds encoding
        time_bounds_name = time_attrs["bounds"]
        if time_bounds_name not in ds:
            raise ValueError(
                f"Time bounds variable '{time_bounds_name=}' is missing from dataset. Expected 'time_bounds'"
            )

        # Check time_bounds structure
        time_bounds = ds[time_bounds_name]
        if len(time_bounds.dims) != 2 or time_bounds.dims[0] != "time":
            raise ValueError(
                f"Time bounds, '{time_bounds_name=}', has incorrect dimensions, {time_bounds.dims}."
                "Expected two dimensions: ('time', <second_dimension>)."
            )

        # Check that the second dimension length is 2 (indicating time bounds)
        if time_bounds.shape[1] != 2:
            raise ValueError(
                f"Time bounds '{time_bounds_name}' has incorrect shape {time_bounds.shape}. "
                "The second dimension should have length 2 to represent time bounds."
            )

        # Check for the correct 'long_name' attribute for time_bounds
        if (
            "long_name" not in time_bounds.attrs
            or time_bounds.attrs["long_name"] != "time_bounds"
        ):
            raise ValueError(
                f"Time bounds '{time_bounds_name}' is missing its 'long_name':'time_bounds' attribute."
            )

        return ds

    @field_validator("ds")
    @classmethod
    def lat_dim(cls, ds: xr.Dataset) -> xr.Dataset:
        # Check that the dataset has a properly set-up latitude dimension
        lat_names = {"lat", "latitude", "y"}
        dims = ds.dims
        dims_lower = {
            dim.lower(): dim for dim in dims
        }  # Map lowercased dims to original names
        lat_names_found = [
            dims_lower[name.lower()] for name in lat_names if name.lower() in dims_lower
        ]

        # Ensure there is only one latitude dimension
        if len(lat_names_found) != 1:
            raise ValueError(
                f"Dataset has {len(lat_names_found)} latitude dimensions, expected exactly one. Found: {lat_names_found}"
            )

        lat_name = lat_names_found[0]

        # Check that one of the accepted latitude long_names exists
        lat_var = ds[lat_name]
        lat_attrs = lat_var.attrs

        # Check for missing latitude attributes
        missing = set(["axis", "long_name", "standard_name", "units"]) - set(lat_attrs)
        if missing:
            raise ValueError(
                f"Dataset is missing latitude-specific attributes, {missing=}"
            )
        else:
            # Check axis
            if lat_attrs["axis"] != "Y":
                raise ValueError(
                    f"Incorrect latitude axis attribute: {lat_attrs['axis']}. Expected 'Y' (case sensitive)."
                )
            # Check standard_name
            if lat_attrs["standard_name"] != "latitude":
                raise ValueError(
                    f"Incorrect latitude standard_name attribute: {lat_attrs['standard_name']}. Expected 'latitude' (case sensitive)."
                )
            # Check units
            valid_lat_units = {
                "degrees_north",
                "degree_north",
                "degree_N",
                "degrees_N",
                "degreeN",
                "degreesN",
            }
            lat_units = lat_attrs.get("units")
            if lat_units not in valid_lat_units:
                raise ValueError(
                    f"Invalid 'units' attribute for latitude dimension. Found: {lat_units}. Expected one of {valid_lat_units}."
                )

        return ds

    @field_validator("ds")
    @classmethod
    def lon_dim(cls, ds: xr.Dataset) -> xr.Dataset:
        # Check that the dataset has a properly set-up longitude dimension
        lon_names = {"lon", "longitude", "x"}
        dims = ds.dims
        dims_lower = {
            dim.lower(): dim for dim in dims
        }  # Map lowercased dims to original names
        lon_names_found = [
            dims_lower[name.lower()] for name in lon_names if name.lower() in dims_lower
        ]

        # Ensure there is only one longitude dimension
        if len(lon_names_found) != 1:
            raise ValueError(
                f"Dataset has {len(lon_names_found)} longitude dimensions, expected exactly one. Found: {lon_names_found}"
            )

        lon_name = lon_names_found[0]

        # Check that one of the accepted longitude long_names exists
        lon_var = ds[lon_name]
        lon_attrs = lon_var.attrs

        # Check for missing longitude attributes
        missing = set(["axis", "long_name", "standard_name", "units"]) - set(lon_attrs)
        if missing:
            raise ValueError(
                f"Dataset is missing longitude-specific attributes, {missing=}"
            )
        else:
            # Check axis
            if lon_attrs["axis"] != "X":
                raise ValueError(
                    f"Incorrect latitude axis attribute: {lon_attrs['axis']}. Expected 'X' (case sensitive)"
                )
            # Check standard_name
            if lon_attrs["standard_name"] != "longitude":
                raise ValueError(
                    f"Incorrect latitude standard_name attribute: {lon_attrs['standard_name']}. Expected 'longitude' (case sensitive)"
                )

            # Check units
            valid_lon_units = {
                "degrees_east",
                "degree_east",
                "degree_E",
                "degrees_E",
                "degreeE",
                "degreesE",
            }
            lon_units = lon_attrs.get("units")
            if lon_units not in valid_lon_units:
                raise ValueError(
                    f"Invalid 'units' attribute for longitude dimension. Found: {lon_units}. Expected one of {valid_lon_units}."
                )

        return ds


if __name__ == "__main__":
    dset = xr.open_dataset(sys.argv[1])
    test = ILAMBDataset(ds=dset)
