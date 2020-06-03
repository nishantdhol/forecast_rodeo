#!/usr/bin/env python
# coding: utf-8

# # Batch 2011-2018 backward stepwise
# 
# Generate MultiLLR (local linear regression with multitask model selection) forecasts for all dates, 2011-2018, using the batch cluster or, optionally, local sequential execution.

# In[1]:


# Autoreload packages that are modified
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

# Load relevant packages
import numpy as np
import pandas as pd
from sklearn import *
import sys
import subprocess
from datetime import datetime, timedelta
import netCDF4
import time
from functools import partial
import os

if os.path.basename(os.getcwd()) == "experiments":
    os.chdir(os.path.join("..",".."))

# Adds 'experiments' folder to path to load experiments_util
sys.path.insert(0, 'src/experiments')
# Load general utility functions
from experiments_util import *
# Load functionality for fitting and predicting
from fit_and_predict import *
# Load functionality for evaluation
from skill import *
# Load functionality for stepwise regression
from stepwise_util import *


# In[2]:


#
# Choose experiment parameters
#
gt_id = "contest_precip" # "contest_precip" or "contest_tmp2m"
target_horizon = "56w" # "34w" or "56w"
margin_in_days = 56
criterion = "mean"

# If run_locally is False, forecast generation jobs for each target date 
# are submitted to a batch cluster using batch_script (recommended)
# If run_locally is True, forecast generation for each target data is 
# executed locally and sequentially and the setting of batch_script is irrelevant
run_locally = True
# Shell script for submitting batch job to cluster; please change to your personal 
# batch cluster submission script.
# Usage for our script is:
#   src/batch/quick_sbatch_python script.py\ script_arg1\ script_arg2 num_cores mem
batch_script = 'src/batch/quick_sbatch_python.sh'
num_cores = 4
mem = "8GB"

contest_id = get_contest_id(gt_id, target_horizon)

#
# Create list of submission dates in YYYYMMDD format
#
submission_dates = [datetime(y,4,18)+timedelta(14*i) for y in range(2011,2018) for i in range(26)]
submission_dates = ['{}{:02d}{:02d}'.format(date.year, date.month, date.day) for date in submission_dates]


# In[ ]:


#
# Generate forecasts for each date by submitting batch job or executing locally
#
fitting_script = 'src/experiments/2011-2018_backward_stepwise.py'
for submission_date_str in submission_dates:
    # Load result file name for checking convergence for this submission date
    file_name = default_result_file_names(
        gt_id = gt_id, 
        target_horizon = target_horizon, 
        margin_in_days = margin_in_days,
        criterion = criterion,
        submission_date_str = submission_date_str,
        procedure = "backward_stepwise",
        hindcast_folder = False,
        hindcast_features = False,
        use_knn1 = False)["converged"]
    file_name = file_name.replace("contest_period", "2011-2018")
    # Only run job if result file does not yet exist
    if not os.path.exists(file_name):        
        if run_locally:
            # Execute command locally
            cmd = "{} {} {} {} {} {} {} {} {}".format(
                "python", fitting_script, 
                gt_id, target_horizon,  
                margin_in_days, criterion,
                False, submission_date_str, num_cores)
        else:
            # Submit batch job
            cmd = r"""{} {}\ {}\ {}\ {}\ {}\ {}\ {}\ {} {} {}""".format(
                batch_script, fitting_script, 
                gt_id, target_horizon, 
                margin_in_days, criterion,
                False,
                submission_date_str, num_cores,
                num_cores, mem)
        print cmd
        os.system(cmd)


# In[ ]:




