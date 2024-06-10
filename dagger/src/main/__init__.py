"""This is a dagger module to chain data-science operation in a reproducible environment.

You can chain python code and GPTScript prompts in any combination you want.


```bash
dagger call import-data --working-dir ./working_dir --api-keys ~/kaggle.json --competition Titanic \
  preprocess-data --task-dir ./preprocess1 --name preprocess1 \
  preprocess-data --task-dir ./preprocess2 --name preprocess2 \
  preprocess-data --task-dir ./preprocess3 --name preprocess3 \
  jupyterlab --jupyter-secret env:TOKEN as-service up
```

"""

from typing import Self
from dagger import dag, function, object_type
from dagger.client.gen import Container, Directory, File, Secret


@object_type
class KaggleDagger:

    labcontainer: Container = dag.container().from_("quay.io/jupyter/scipy-notebook:latest")
    working_dir:    Directory

    @function
    def debug(self) -> Container:
        "Return the container for debug"

        return (self.labcontainer
                .with_entrypoint(["bash"])
        )

    @function
    async def jupyterlab(self, jupyter_secret: Secret ) -> Container:
        """Returns a service running a JupyterLab instance on working dir"""

        token = await (jupyter_secret.plaintext())

        return (
            self.labcontainer
            .with_exposed_port(8888)
            .with_entrypoint(["tini", "-g", "--", "start.sh"])
            .with_secret_variable("JLAB_SECRET", jupyter_secret)
            .with_exec(["start-notebook.py", f"--IdentityProvider.token={token}"])
        )


    @function
    def import_data(self, api_keys: File, competition: str) -> Self:
        """Create a container with my data"""
        
        self.labcontainer = (
            self.labcontainer
            .with_mounted_directory("/home/jovyan/work", self.working_dir, owner="jovyan")
            .with_mounted_file("/home/jovyan/.kaggle/kaggle.json", api_keys)
            .with_exec(["pip3","install","kaggle"])
            .with_exec(["kaggle","competitions","download","-c", competition])
            .with_exec(["unzip", f"{competition}.zip"])
        )

        return self

    @function
    def preprocess_data(self, name: str, task_dir: Directory) -> Self:
        """Execute a python script passed from task dir with deps into requirements.txt"""

        self.labcontainer = (
            self.labcontainer
            .with_mounted_directory(f"/home/jovyan/work/{name}", task_dir, owner="jovyan")
            #.with_mounted_file(f"/opt/process_{name}/requirements.txt", requirements_file)
            #.with_mounted_file(f"/opt/process_{name}/main.py", python_main)
            .with_workdir(f"/home/jovyan/work/{name}")
            .with_exec(["pip3", "install", "-r", f"requirements.txt"])
            .with_exec(["python3", f"main.py"])
        )

        return self


    @function
    def preprocess_gpt(self, name: str, gpt_file: File, openai_token: Secret) -> Self:
        """Execute a gptScript file with the given openai token and duckDB available for data analysis operation"""
        self.labcontainer = (
            self.labcontainer
            .with_mounted_file(f"/opt/process_{name}/main.gpt", gpt_file)
            .with_exec(["pip3", "install", f"gptscript"], skip_entrypoint=True)
            .with_exec(["wget", "https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip"], skip_entrypoint=True)
            .with_exec(["unzip", "duckdb_cli-linux-amd64.zip"], skip_entrypoint=True)
            .with_user("root")
            .with_exec(["mv", "duckdb", "/usr/bin/"], skip_entrypoint=True)
            .with_user("jovyan")
            .with_secret_variable("OPENAI_API_KEY", openai_token)
            .with_exec(["bash", "-c", f"gptscript /opt/process_{name}/main.gpt || echo OPS"])
        )

        return self

