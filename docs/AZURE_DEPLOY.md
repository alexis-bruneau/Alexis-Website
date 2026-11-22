# Automatic deploy to Azure

Written for someone who uses the Azure portal and GitHub in the browser, not the command line.

**What this does:** when a change that affects the web app is merged to `master`, GitHub builds the Docker image inside your
Azure container registry and updates the Container App, so https://quartierinsights.ca shows the change. The workflow is
`.github/workflows/deploy_azure.yml`.

**What you do once** (parts 1 to 4 below), before the pull request is merged: switch the app to Multiple revision mode, create
an identity for GitHub in Azure, give it a role, and store three IDs in GitHub.

Menu names in the Azure portal change from time to time. If a name below is not on your screen, look for the closest one,
and send a screenshot (see "Screenshots").

## Screenshots

You may send screenshots. Before you do, **hide**: any secret value, any value under "Secrets" in the Container App, anything
that looks like `AccountKey=...` or `DefaultEndpointsProtocol=...`. The three IDs below are identifiers, not passwords, and are
fine to show, but hiding them costs nothing. **Never type or paste the storage connection string anywhere in GitHub, in a
file in the repo, or in chat.** This setup does not need it: the Container App already holds it in its secret `conn`, and the
deploy never touches that secret.

## How a deploy works

```
merge to master
  -> GitHub starts "Deploy to Azure" and asks Azure to trust it for this run (OpenID Connect, no stored password)
  -> az acr build   builds the image in acralexisbruneau, tagged with the commit SHA (never "latest")
  -> a NEW revision of ca-alexis-web is created with 0% of the visitors and the new image
  -> the workflow tests the new revision on its own web address (/ and /ottawa_map must answer 200)
  -> 100% of the visitors move to the new revision
  -> the workflow tests https://quartierinsights.ca/, /ottawa_map and https://www.quartierinsights.ca/
  -> every other revision is switched off (deactivated), so only one copy of the app runs and is billed
```

If anything fails before that last step, the visitors go back to the previous revision by themselves and the job turns
red. A failed deploy therefore leaves the site on the old version.

**Cost:** while a deploy runs, two copies of the app run for a few minutes (the old and the new). Afterwards only one runs, so the
monthly bill stays about what it is today. A deactivated revision costs nothing and is kept, so you can activate it again to roll back.

A **revision** is one version of the app (an image plus its settings). In **Multiple** revision mode several revisions can
exist at once and you decide what share of visitors each gets. In Single mode Azure keeps only the newest.

The workflow runs only when something that goes into the image changes: `app/`, `templates/`, `static/`, `redfin/` (except its
`docs`, `tests`, `scripts` and `data/queue`, which are not copied into the image anyway), `requirements.txt`, `Dockerfile`,
`.dockerignore`, and the workflow files themselves. A change to `README`, `docs/` or `build_site.py` does not deploy.

---

## Part 1: switch the Container App to Multiple revision mode

Why: the workflow tests the new version before visitors see it, and the old version stays available for rollback. Both need Multiple mode.
The workflow refuses to run (with a clear message) if the app is still in Single mode.

1. Portal: search **Container Apps** → **ca-alexis-web**.
2. Left menu: **Application** → **Revisions** (in some versions: **Revision management**).
3. Find **Revision mode** (a dropdown or a "Choose revision mode" button). Select **Multiple** and **Apply** / **Save**.
4. **Check:** the page now shows a revisions table with a **Traffic** column. Your current revision (the one running `v1`) is
   listed with 100%. Open https://quartierinsights.ca/ in the browser: it still works. Switching the mode does not restart the app.

## Part 2: create the identity GitHub will use (app registration)

An *app registration* is an identity for a program. GitHub will log in as this identity.

1. Portal: search **Microsoft Entra ID** → left menu **App registrations** → **+ New registration**.
2. **Name:** `gh-alexis-website-deploy`. **Supported account types:** "Accounts in this organizational directory only"
   (single tenant). **Redirect URI:** leave empty. Click **Register**.
3. You land on the app's **Overview**. Two values are shown that you will need in part 4:
   **Application (client) ID** and **Directory (tenant) ID**. Do not copy them anywhere public.
4. **Check:** the app `gh-alexis-website-deploy` appears under App registrations → "Owned applications".

Do **not** create a client secret ("Certificates & secrets" → "Client secrets"). The point of this setup is that no password exists.

## Part 3: tell Azure to trust GitHub (federated credential)

This says: "a run of the repository `alexis-bruneau/Alexis-Website` on the `master` branch may log in as this identity". A run
from any other branch or repository is refused.

1. Open the app registration → left menu **Certificates & secrets** → tab **Federated credentials** → **+ Add credential**.
2. **Federated credential scenario:** `GitHub Actions deploying Azure resources`.
3. Fill in:
   - **Organization:** `alexis-bruneau`
   - **Repository:** `Alexis-Website`
   - **Entity type:** `Branch`
   - **GitHub branch name:** `master`
   - **Name:** `github-master`
   - Leave **Audience** as it is (`api://AzureADTokenExchange`).
4. **Check:** the preview line **Subject identifier** reads exactly `repo:alexis-bruneau/Alexis-Website:ref:refs/heads/master`.
   If the repository was renamed or lives under another owner, this line must match the GitHub address. Click **Add**.
5. **Check:** the credential `github-master` is listed.

## Part 4: give the identity a role

A *role assignment* says who may do what, and where. Recommended:

| Role | Scope | Who | Why |
|---|---|---|---|
| **Contributor** | resource group **rg-alexis-web** | `gh-alexis-website-deploy` | Lets it build in the registry (`az acr build`), push the image, create revisions and change traffic on `ca-alexis-web`. |

Why this is still least privilege: the scope is one resource group, so the identity cannot see or change anything else in your
subscription, and Contributor cannot give anyone else access (that needs Owner). Everything the workflow does (registry,
Container App, revisions) lives in that group.

Narrower roles (for example only on the registry and only on the Container App) probably work, but I could not verify the
exact minimum set without running it against Azure, and a wrong guess would fail the deploy in the middle. Start with
Contributor on the resource group; narrowing is an optional experiment for later.

1. Portal: **Resource groups** → **rg-alexis-web** → left menu **Access control (IAM)**.
2. **+ Add** → **Add role assignment**.
3. Tab **Role**: search `Contributor`. It is listed under the tab **Privileged administrator roles** (not "Job function roles").
   Select **Contributor** (not "Owner", not "Reader") → **Next**.
4. Tab **Members**: **Assign access to:** `User, group, or service principal` → **+ Select members** → type
   `gh-alexis-website-deploy` → click it so it appears under "Selected members" → **Select** → **Next**.
5. If the portal asks for **Conditions** or **Assignment type**, keep the defaults (permanent, no condition).
   **Review + assign** twice.
6. **Check:** IAM → tab **Role assignments** → search `gh-alexis-website-deploy`: one row, role **Contributor**, scope
   "This resource". Role changes can take a few minutes to take effect.

## Part 5: store the three IDs in GitHub

The workflow reads them as repository **secrets** (GitHub hides their values in logs). They identify who logs in; they cannot
log in by themselves.

Where to find each value:

| GitHub secret name | Where in the portal |
|---|---|
| `AZURE_CLIENT_ID` | Entra ID → App registrations → `gh-alexis-website-deploy` → Overview → **Application (client) ID** |
| `AZURE_TENANT_ID` | same Overview page → **Directory (tenant) ID** |
| `AZURE_SUBSCRIPTION_ID` | Portal search **Subscriptions** → your subscription → **Subscription ID** (the one that contains `rg-alexis-web`) |

Create them:

1. GitHub: the repository **alexis-bruneau/Alexis-Website** → **Settings** → left menu **Secrets and variables** → **Actions**.
2. Tab **Secrets** → **New repository secret**. **Name:** `AZURE_CLIENT_ID`. **Secret:** paste the value. **Add secret**.
3. Repeat for `AZURE_TENANT_ID` and `AZURE_SUBSCRIPTION_ID`. Names must match exactly (capitals, underscores).
4. **Check:** the three names are listed under "Repository secrets". GitHub never shows the values again; that is normal.

## Part 6: the first deploy

The workflow file only exists on `master` after the pull request is merged, and the federated credential only trusts
`master`, so the first run cannot happen earlier. **The merge itself starts the first deploy**, because the pull request
adds the `Dockerfile` and the workflow, which are on the path list.

Before merging, check that parts 1 to 5 are done. Then:

1. Merge the pull request on GitHub.
2. GitHub: tab **Actions** → in the left list **Deploy to Azure** → the newest run (the yellow dot means running).
3. Open the run → click the job **deploy**. Steps appear with a green tick (done), spinner (running) or red cross (failed).
4. **Check:** every step is green; the run's **Summary** page lists the image tag, the live revision and the previous
   revision. In the portal, Container App → Revisions and replicas: **Active revisions** lists only the new revision at 100%, and
   **Inactive revisions** lists the old one (your `v1`). Open https://quartierinsights.ca/ and https://quartierinsights.ca/ottawa_map.

The whole run takes about 8 to 15 minutes the first time (the image build is the long part; the app also needs about a
minute to download its data when a revision starts).

**Testing without touching the app:** after the merge, **Actions** → **Deploy to Azure** → **Run workflow** (right side) →
tick **build_only** → **Run workflow**. It logs in and builds the image only, which tests parts 2 to 5 and the Docker build
without creating a revision. A normal **Run workflow** (box unticked) redeploys the current master.

Pull requests do not deploy: only pushes to `master` and a manual run.

## Reading the Actions log

1. **Actions** → click the run → click **deploy** (left) → click a step to expand its output.
2. Red cross = that step failed; read the last lines of it (the line starting with `Error:` or `::error::`).
3. Common failures:

| Message or symptom | Meaning and fix |
|---|---|
| `AADSTS700213` / `No matching federated identity record found` in "Log in to Azure" | The subject in part 3 does not match the run. Check owner/repo spelling and that it was run from `master`. |
| `Login failed ... client-id` is empty / `Not all values are present` | A secret is missing or misspelled (part 5). |
| `AuthorizationFailed` / `does not have authorization` | The role in part 4 is missing, on the wrong scope, or still propagating (wait 5 minutes and re-run). |
| `is in 'Single' revision mode` | Do part 1. |
| "Build the image" fails with a Docker error | The same error would appear in `docker build` on your PC. Fix and push. |
| "Test the new revision" never gets 200 | The new version does not start (check its logs: Container App → Revisions → the new revision → **Console log**/**System log**, or Monitoring → **Log stream**). Traffic never left the old revision. |
| "Smoke test the public site" fails | The workflow already sent the traffic back to the previous revision (last step "Roll the traffic back"). Look at the log of the new revision, fix, push again. |

Nothing in the log contains the storage connection string: the workflow never reads the secret `conn`.

## Roll back to the previous revision (portal)

Use this when the deploy was green but the site misbehaves.

Only the live revision runs; every older one is inactive (stopped, free, kept). So a rollback has two parts: start the old
revision, then move the visitors to it.

1. Portal: **Container Apps** → **ca-alexis-web** → **Revisions and replicas**.
2. Tab **Inactive revisions**. Revision names end with the commit's first 7 characters; the previous one is also named in the
   run's **Summary** in GitHub (Actions → the run → Summary). Select it and click **Activate** (in the panel on the right, or
   the toolbar). Confirm if asked.
3. **Check:** go back to **Active revisions** and click **Refresh** until the old revision shows **Running**. This takes
   about a minute (the app downloads its data when it starts). It has **0%** traffic for now, so visitors are unaffected.
4. In **Active revisions**, in the **Traffic** column set the old revision to **100** and the new (bad) one to **0**, then **Save**.
5. **Check:** reload https://quartierinsights.ca/ and https://quartierinsights.ca/ottawa_map; the old behaviour is back.
6. Optional, to stop paying for the bad revision: select it and click **Deactivate**.

Notes:
- To go back further than one deploy, activate any older revision in the same way (the list keeps them). Or revert the change
  on `master`: a revert is a normal commit, so it deploys itself (about 10 minutes).
- The next merge to master deploys normally and takes the traffic again, even after a rollback, and then deactivates the
  revision you rolled back to. If you want the old version to stay until a fix is ready, turn the automation off first (below).
- Every image stays in the registry under its commit SHA (Container registries → acralexisbruneau → Repositories →
  `alexis-website`), so nothing you ever deployed is lost. Images use storage space (about 200 MB each; the Basic registry
  includes 10 GiB).
- Rolling back is slower than switching traffic between two running copies: that is the price of not paying for a second copy.

## Turn the automation off

From gentlest to hardest. Any of them leaves the app running as it is.

1. **Pause the workflow:** GitHub → **Actions** → **Deploy to Azure** → the **…** menu (top right) → **Disable workflow**.
   Re-enable it the same way. Merges to master then no longer deploy.
2. **Cut GitHub's access:** Entra ID → App registrations → `gh-alexis-website-deploy` → Certificates & secrets → Federated
   credentials → delete `github-master`. Runs then fail at "Log in to Azure". Add the credential again to resume.
3. **Remove everything:** delete the role assignment (rg-alexis-web → Access control (IAM) → Role assignments → select the
   row → **Remove**), the app registration, the three GitHub secrets, and finally the file `.github/workflows/deploy_azure.yml`
   in a pull request.

To go back to deploying by hand: set the image tag in the portal (Container App → **Application** → **Containers** →
**Edit and deploy** → pick the container → change the image tag → Save). That creates a new revision like the workflow does.

## What is not covered here

- The daily scrape writes new data to Azure Storage, but the running app loads its data only when it starts. Getting the app
  to pick up new data is a separate step (a protected refresh); until then a deploy or a restart of the revision reloads it.
- `/refresh-data` has no login today. Do not rely on it from outside; it is dealt with in that separate step.
