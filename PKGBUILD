pkgname=codex-accounts
pkgver=0.1.0
pkgrel=1
pkgdesc='CLI for viewing Codex account usage and switching accounts'
arch=('x86_64')
url='https://github.com/sieciowiecxyz/codex-accounts'
license=('MIT')
depends=('gcc-libs')
source=('codex-accounts' 'LICENSE')
sha256sums=('SKIP' 'SKIP')

package() {
  install -Dm755 "$srcdir/codex-accounts" "$pkgdir/usr/bin/codex-accounts"
  install -Dm644 "$srcdir/LICENSE" "$pkgdir/usr/share/licenses/$pkgname/LICENSE"
}
