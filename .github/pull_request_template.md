## Thank you for contributing to ILAMB-Data. Use this template when submitting a pull request.

### 🛠 Summary of changes
✨**Include a reference to the issue # in this section**
Summarize the changes you are making to the repo. Are you fixing a bug? Adding a dataset? Describe what you did in a few sentences or bullet points:

### 🧪 NetCDF Validation
✨**Add an `x` between the brackets to indicate basic testing was completed**
- [ ] Created a new working directory (e.g., `ILAMB-DATA/GFED5/`)
- [ ] Created a standalone python script that downloads and processes the new dataset (e.g., `ILAMB-DATA/GFED5/convert.py`)
    - [ ] The python script outputs a netcdf (e.g., `ILAMB-DATA/GFED5/burntFractionAll_mon_GFED5_199701-202012.nc`)
- [ ] Ran `python3 validate_dataset.py ILAMB-DATA/GFED5/burntFractionAll_mon_GFED5_199701-202012.nc` in command line and resolved any errors
- [ ] Visually inspected the outputted netCDF for obvious issues
    - [ ] `ncdump -h ILAMB-DATA/GFED5/burntFractionAll_mon_GFED5_199701-202012.nc` to visually inspect the netCDF header information
    - [ ] `ncview ILAMB-DATA/GFED5/burntFractionAll_mon_GFED5_199701-202012.nc` to visually inspect the map (where applicable)

### 🧪 (Optional) Preview
✨**Attach an image of your dataset here**

### 🏎 (Optional) Quality Checklist
✨**Add an `x` between the brackets to indicate script quality adherence**

- [ ] There are no unused libraries imported into the code
- [ ] There are no erroneous console logs, debuggers, or leftover testing code
- [ ] There are no hard-coded paths to your local machine in the script
- [ ] Useful headings describe sections of your code
- [ ] Variable names are generalizable and easy to read