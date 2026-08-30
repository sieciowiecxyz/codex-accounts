Name:           codex-accounts
Version:        0.1.0
Release:        1%{?dist}
Summary:        CLI for viewing Codex account usage and switching accounts
License:        MIT
URL:            https://github.com/sieciowiecxyz/codex-accounts
Source0:        codex-accounts
Source1:        LICENSE
BuildArch:      x86_64

%description
codex-accounts displays Codex account usage and switches the active account.

%prep
%build
%install
install -D -m 0755 %{SOURCE0} %{buildroot}%{_bindir}/codex-accounts
install -D -m 0644 %{SOURCE1} %{buildroot}%{_licensedir}/%{name}/LICENSE

%files
%license %{_licensedir}/%{name}/LICENSE
%{_bindir}/codex-accounts
