## Thank you for contributing to ILAMB-Data. Use this template when submitting a pull request.

### 🛠 Issue reference
✨**Include a reference to the issue # here.**
Summarize the changes you are making to the repo. Are you fixing a bug? Adding a dataset? Describe what you did in a few sentences or bullet points.

### 🧪 Testing
✨**Add an `x` between the brackets to indicate basic testing was completed**
- [ ] I inspected the outputted NetCDF and checked for obvious errors
- [ ] I visualized the outputted NetCDF at a timestamp to check for obvious visual errors
- [ ] I compared my `convert` script to recent existing ones
- [ ] I attempted to create/encode the NetCDF according to CF Compliance guidelines

### 🧪 (Optional) Preview
✨**Attach an image of your dataset here**

### 🏎 (Optional) Quality Checklist
✨**Add an `x` between the brackets to indicate script quality adherence**

- [ ] There are no unused libraries imported into the code
- [ ] There are no erroneous console logs, debuggers, or leftover testing code
- [ ] There are no hard-coded paths to your local machine in the script
- [ ] Useful headings describe sections of your code
- [ ] Variable names are generalizable and easy to read

### 📏 (Optional) CF Compliance In-Depth Checklist
✨**Add an `x` between the brackets to ensure CF compliance**

#### Dimensions
- [ ] Dimensions include `time` with attributes/encoding:
    - [ ] `axis` attribute is `T`
    - [ ] `units` attribute/encoding is `days since YYYY-MM-DD HH:MM:SS`
    - [ ] `long_name` attribute is `time`
    - [ ] `calendar` encoding is `noleap`
    - [ ] `bounds` encoding is `time_bounds`
- [ ] Dimensions include `lon` with attributes:
    - [ ] `axis` attribute is `X`
    - [ ] `units` attribute is `degrees_east`
    - [ ] `long_name` attribute is  `longitude`
- [ ] Dimensions include `lat` with attributes:
    - [ ] `axis` attribute is `Y`
    - [ ] `units` attribute is `degrees_north`
    - [ ] `long_name` attribute is `latitude`
- [ ] Dimensions include `nv`, which is an array of length `2` that contains the start date and end date bounding the dataset

#### Data Variables and their Attributes
- [ ] **The variable(s) for model comparison are present**
    - [ ] the variables are linked to the `time`,`lat`, and `lon` dimensions
    - [ ] `long_name` attribute is specified
    - [ ] `units` attribute is specified
    - [ ] (If necessary) `ancillary_variables` attribute is specified if an uncertainty value is provided
    - [ ] (Optional) Float32 data type
    - [ ] (Optional) No-data values masked as NaN
- [ ] **If applicable, a data uncertainty variable is present** (e.g., standard_deviation or standard_error)
    - [ ] the variable is linked to the `time`, `lat`, and `lon` dimensions
    - [ ] `long_name` attribute is specified (e.g., cSoil standard_deviation)
    - [ ] `units` attribute is specified; it is unitless, so it should be `1`
- [ ] **A time_bounds variable is present**
    - [ ] the variable is linked to the `time` and `nv` dimensions
    - [ ] `long_name` attribute is specified as `time_bounds`
    - [ ] `units` is encoded as `days since YYYY-MM-DD HH:MM:SS`

#### Global Attributes
- [ ] 'title'
- [ ] 'institution'
- [ ] 'source'
- [ ] 'history'
- [ ] 'references'
    - [ ] @reference-type{ref-name, author={Last, First and}, title={}, journal={}, year={}, volume={}, pages={num--num}, doi={no-https}}
- [ ] 'comment'
- [ ] 'Conventions'