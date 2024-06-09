"""A generated module for KaggleDagger functions

This module has been generated via dagger init and serves as a reference to
basic module structure as you get started with Dagger.

Two functions have been pre-created. You can modify, delete, or add to them,
as needed. They demonstrate usage of arguments and return types using simple
echo and grep commands. The functions can be called from the dagger CLI or
from one of the SDKs.

The first line in this comment block is a short description line and the
rest is a long description with more detail on the module's purpose or usage,
if appropriate. All modules should have a short description.
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
        
