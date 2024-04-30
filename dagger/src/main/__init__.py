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

    @function
    async def jupyterlab(self, jupyter_secret: Secret ) -> Container:
        """Returns a service running a JupyterLab instance on working dir"""

        token = await (jupyter_secret.plaintext())

        return (
            self.labcontainer
            .with_exposed_port(8888)
            .with_entrypoint(["tini", "-g", "--", "start.sh"])
            .with_exec(["start-notebook.py", f"--IdentityProvider.token={token}"])
        )


    @function
    def import_data(self, api_keys: File, working_dir: Directory, competition: str) -> Self:
        """Create a container with my data"""
        
        self.labcontainer = (
            self.labcontainer
            .with_mounted_directory("/home/jovyan/work", working_dir, owner="jovyan")
            .with_mounted_file("/home/jovyan/.kaggle/kaggle.json", api_keys)
            .with_exec(["pip3","install","kaggle"])
            .with_exec(["kaggle","competitions","download","-c", competition])
            .with_exec(["unzip", f"{competition}.zip"])
        )

        return self

    @function
    def preprocess_data(self, name: str, working_dir: Directory, requirements_file: File, python_main: File) -> Self:

        self.labcontainer = (
            self.labcontainer
            .with_mounted_directory(f"/home/jovyan/work/{name}", working_dir, owner="jovyan")
            .with_mounted_file(f"/opt/process_{name}/requirements.txt", requirements_file)
            .with_mounted_file(f"/opt/process_{name}/main.py", python_main)
            .with_workdir(f"/home/jovyan/work/{name}")
            .with_exec(["pip3", "install", "-r", f"/opt/process_{name}/requirements.txt"])
            .with_exec(["python3", f"/opt/process_{name}/main.py"])
        )

        return self
        
