{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    alejandra = {
      inputs.nixpkgs.follows = "nixpkgs";
      url = "github:kamadorueda/alejandra/3.0.0";
    };
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    alejandra,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShell = pkgs.mkShell {
          buildInputs =
            [alejandra.defaultPackage.${system} pkgs.nodejs-14_x pkgs.dotnet-sdk_6]
            ++ (
              if pkgs.stdenv.isDarwin
              then [pkgs.darwin.apple_sdk.frameworks.CoreServices]
              else []
            );
        };
      }
    );
}
