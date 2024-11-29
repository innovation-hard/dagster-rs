ARG base_image
FROM ${base_image}

ARG repo_path
RUN rm -rf /${repo_path}
COPY infoxel/ml/${repo_path}/ /${repo_path}/
RUN pip install /${repo_path}
RUN git checkout main
