"""3. EXECUTE: the commands you run. They only start the steps of extract/ and transform/ (as subprocesses), check the result and print a summary.

    python -m redfin.run.daily --limit-sold 150 --limit-active 150     # the once-a-day command, with how many listings to take
    python -m redfin.run.daily --transform-only                        # rebuild silver from the bronze already saved (no redfin.ca)
    python -m redfin.run.verify                                        # read-only: is silver as new as bronze and the census?

Lineage: extract/ (1. scrape redfin.ca -> bronze)  ->  transform/ (2. bronze -> silver)  ->  run/ (3. orchestrates, checks, reloads the app).
"""
