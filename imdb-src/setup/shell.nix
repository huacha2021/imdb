{ pkgs ? import <nixpkgs> {} }:
let
  python-env = pkgs.python310.withPackages (ps: with ps; [ pip ]);
in
(pkgs.buildFHSUserEnv {
  name = "env";
  targetPkgs = pkgs: with pkgs; [ 
    python-env
  ];
  runScript = "bash";
  profile = ''
    export TMPDIR=$(pwd)/_build/tmp
    export BUILDDIR=$(pwd)/_build/
    export PIP_PREFIX=$(pwd)/_build/pip_packages
    export PYTHONPATH="$PIP_PREFIX/${python-env.sitePackages}:$PYTHONPATH"
    export PATH="$PIP_PREFIX/bin:$PATH"
    unset SOURCE_DATE_EPOCH
  '';
}).env
