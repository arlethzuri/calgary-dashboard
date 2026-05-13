To run demo3 or simple_subsection, follow these steps:

1. Create and activate the Conda environment:
   ```
   conda env create -f calgary_dev.yml
   conda activate calgary_dev
   ```

2. Install the project in editable mode:
   ```
   pip install -e .
   ```


3a. Launch the Bokeh server for demo3:
   ```
   bokeh serve demos/demo3/app.py --show
   ```
3b. Run demo.ipynb using calgary_dev Python kernel.