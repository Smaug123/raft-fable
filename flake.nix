{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    alejandra = {
      inputs.nixpkgs.follows = "nixpkgs";
      url = "github:kamadorueda/alejandra/3.0.0";
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    alejandra,
    ...
  }: {
    devShell.aarch64-darwin = let
      system = "aarch64-darwin";
    in let
      pkgs = nixpkgs.legacyPackages.aarch64-darwin;
    in
      pkgs.mkShell {
        buildInputs = [alejandra.defaultPackage.aarch64-darwin pkgs.nodejs-14_x pkgs.dotnet-sdk_6 pkgs.darwin.apple_sdk.frameworks.CoreServices];
      };
  };
}
