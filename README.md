ILAMB-Data
==========

This repository stores the scripts used to download observational data from various sources and format it in a [CF-compliant](http://cfconventions.org/), netCDF4 file which can be used for model benchmarking via [ILAMB](https://github.com/rubisco-sfa/ILAMB).

Please note that the repository contains no data. If you need to download our observational data, please see the [`ilamb-fetch`](https://www.ilamb.org/doc/ilamb_fetch.html) tutorial. This collection of scripts is to:

* archive how we have produced the datasets compared to models with ILAMB
* expose the details of our formatting choices for transparency
* provide the community a path to contributing new datasets as well as pointing out errors in the current collection

Contributing
============

If you have an suggestion or issue with the observational data ILAMB uses, we encourage you to use the issue tracker associated with this repository rather than that of the ILAMB codebase. This is because the ILAMB codebase is meant to be a general framework for model-data intercomparison and ignorant of the source of the observational data. Here are a few ways you can contribute to this work:

* If you notice an irregularity/bug/error with a dataset in our collection, please raise an issue here with the tag `bug`. We also welcome pull requests which fix these errors, but please first raise an issue to give a record and location where we can have a dialog about the issue.
* If you know of a dataset which would be a great addition to ILAMB, raise an issue here with the tag `enhancement`. Please provide us with details of where we can find the dataset as well as some reasoning for the recommendation.
* We also encourage pull requests with scripts that encode new datasets and will provide more information about procedure in the next section.

Formatting Guidelines
=====================

We appreciate the community interest in improving ILAMB. We believe that more quality observational constraints will lead to a better Earth system model ecosystem and so are always interested in new observational data. We ask that you follow this procedure.

* Before you encode the dataset, you should first search the open and closed issues here on the issue tracker. It may be we have someone already assigned to work on this and do not want to waste your effort. It may also be that we have considered adding the dataset and have a reason its quality is not sufficient.
* If no issue is found, raise a new issue with the tag `enhancement`. This will allow for some discussion and let us know you intend on doing the work.
* You may use any language you wish to encode the dataset, but we strongly encourage you to use python3 if at all possible. You can find examples in this repository to use as a guide. See this [tutorial](https://www.ilamb.org/doc/format_data.html) for details and feel free to ask questions in the issue corresponding to the dataset you are adding.
* Once you have formatted the dataset, we recommend running it against a collection of models and along with other relevant observational datasets using ILAMB. There are [tutorials](https://www.ilamb.org/doc) to help you do this. This will allow the community to evaluate the new addition and decide on if or how it should be included into the curated collection.
* After you have these results, attend one of our conference calls where you can present the results of the intercomparison and the group can discuss. Once the group agrees, then you can submit a pull request and your addition will be included.




