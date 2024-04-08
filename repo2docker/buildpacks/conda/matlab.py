import os
import requests

UBUNTU_NAMES_FOR_BASE_IMAGES = {
    "buildpack-deps:jammy": "ubuntu22.04",
}

def matlab_requirements_scripts(release, base_image):
    """Get the requirements scripts for a MATLAB release and base image
    
    Based on https://github.com/mathworks-ref-arch/matlab-integration-for-jupyter/blob/a4ac9b9ce5c2880596c77670eddab93f7ef9d4fc/matlab/Dockerfile#L77-L88
    """
    _, image_name = os.path.split(base_image)
    if image_name not in UBUNTU_NAMES_FOR_BASE_IMAGES:
        raise ValueError(f"Unknown base image: {base_image}")
    ubuntu_name = UBUNTU_NAMES_FOR_BASE_IMAGES[image_name]
    lrelease = release.lower()
    url = f"https://raw.githubusercontent.com/mathworks-ref-arch/container-images/main/matlab-deps/{lrelease}/{ubuntu_name}/base-dependencies.txt"
    resp = requests.get(url)
    resp.raise_for_status()
    base_apt_packages = ["wget", "unzip", "ca-certificates", "xvfb", "git"]
    apt_packages = [line for line in resp.text.splitlines() if not line.strip().startswith("#")]
    all_package_names = " ".join(base_apt_packages + apt_packages)
    script = f"""export DEBIAN_FRONTEND=noninteractive && apt-get update \
    && apt-get install --no-install-recommends -y {all_package_names} \
    && apt-get clean \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*
"""
    return [("root", script)]

def matlab_installation_scripts(release, products, dest_dir):
    """Get the installation scripts for a MATLAB release and products
    
    Based on https://github.com/mathworks-ref-arch/matlab-integration-for-jupyter/blob/a4ac9b9ce5c2880596c77670eddab93f7ef9d4fc/matlab/Dockerfile#L90-L98
    """
    all_products = list(products) if products is not None else []
    if "MATLAB" not in all_products:
        all_products = ["MATLAB"] + all_products
    products_list = " ".join(all_products)
    script = f"""
wget -q https://www.mathworks.com/mpm/glnxa64/mpm && \ 
    chmod +x mpm && \
    ./mpm install \
    --release={release} \
    --destination={dest_dir} \
    --products {products_list} && \
    rm -f mpm /tmp/mathworks_root.log && \
    ln -s {dest_dir}/bin/matlab /usr/local/bin/matlab
"""
    return [("root", script)]

def matlab_python_engine_installation_scripts(release, dest_dir):
    """Get the installation scripts for the MATLAB engine for Python
    
    Based on https://github.com/mathworks-ref-arch/matlab-integration-for-jupyter/blob/a4ac9b9ce5c2880596c77670eddab93f7ef9d4fc/matlab/Dockerfile#L100-L108
    """
    script = f"""export DEBIAN_FRONTEND=noninteractive && apt-get update \
    && apt-get install --no-install-recommends -y  python3-distutils \
    && apt-get clean \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/* \
    && cd {dest_dir}/extern/engines/python \
    && python setup.py install || true"""
    return [("root", script)]

def matlab_proxy_installation_scripts():
    return [
        ("${NB_USER}", "python -m pip install jupyter-matlab-proxy"),
    ]
