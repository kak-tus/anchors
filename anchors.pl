#!/usr/bin/perl

use strict;
use warnings;
use v5.10;
use utf8;

our $VERSION = 0.4;

use Getopt::Long;
use Pod::Usage;

process();

sub process {
  my $file = undef;
  my $keyword = undef;
  my $sites_limit = 50;
  my $pps_limit = 200;
  my $yandex_key = undef;
  my $yandex_user = undef;
  my $version = undef;

  GetOptions(
    'file|f=s' => \$file,
    'keyword|k=s' => \$keyword,
    'sites=i' => \$sites_limit,
    'pages=i' => \$pps_limit,
    'yandex_key|y=s' => \$yandex_key,
    'yandex_user|u=s' => \$yandex_user,
    'version|v' => \$version,
  );

  if ( $version ) {
    say $VERSION;
    return;
  }

  if ( !( $file && $keyword && $yandex_key && $yandex_user ) ) {
    pod2usage( 1 );
    return;
  }

  my $anch = Anchors->new();
  $anch->process( $keyword, $file, $sites_limit, $pps_limit, $yandex_key, $yandex_user );

  return;
}

package Anchors;

use strict;
use warnings;
use v5.10;
use utf8;

use LWPx::ParanoidAgent;
use HTML::Strip;
use Encode;
use HTML::Restrict;
use Data::Dumper;
use POSIX;
use Clone qw( clone );

use fields qw( ua keyword type file sites_limit pps_limit pages_processed yandex_key yandex_user );

sub new {
  my $proto = shift;

  my $self = fields::new( $proto );

  my $ua = LWPx::ParanoidAgent->new();
  $ua->default_header( 'Accept-Encoding' => 'utf8' );
  $ua->timeout( 10 );

  $self->{ua} = $ua;
  $self->{pages_processed} = {};

  return $self;
}

sub process {
  my __PACKAGE__ $self = shift;
  my $keyword = shift;;
  my $file = shift;
  my $sites_limit = shift;
  my $pps_limit = shift;
  my $yandex_key = shift;
  my $yandex_user = shift;

  binmode STDOUT, ':utf8';
  binmode STDERR, ':utf8';
  $| = 1;

  $self->{keyword} = decode_utf8( $keyword );
  $self->{file} = $file;
  $self->{sites_limit} = $sites_limit;
  $self->{pps_limit} = $pps_limit;
  $self->{yandex_key} = $yandex_key;
  $self->{yandex_user} = $yandex_user;

  $self->_generate();

  return;
}

sub _generate {
  my __PACKAGE__ $self = shift;

  my $sites = $self->_get_sites();
  return unless $sites;

  my $anchors = $self->_get_anchors( $sites );

  my $fln = $self->{file};
  open my $fl, '>' . $fln;
  binmode $fl, ':utf8';

  foreach my $type ( keys %$anchors ) {
    say $fl $type;

    foreach my $anch ( keys %{ $anchors->{ $type } } ) {
      say $fl $anch;
    }

    say $fl "\n";

    say "Найдено $type:" . scalar( keys %{ $anchors->{ $type } } );
  }

  close $fl;

  return;
}

sub _get_sites {
  my __PACKAGE__ $self = shift;

  my $keyword = $self->{keyword};
  my $type = $self->{type};
  my $ua = $self->{ua};
  my $sites_limit = $self->{sites_limit};
  my $yandex_key = $self->{yandex_key};
  my $yandex_user = $self->{yandex_user};

  print "Получение данных из поиска Yandex...";

  my $str = $keyword;

  my $pages = [];
  my $progress = '';
  my $limit = ceil( $sites_limit / 10 );

  for my $cnt ( 0 .. $limit ) {
    my $rsp = $ua->get( "http://xmlsearch.yandex.ru/xmlsearch?user=$yandex_user&key=$yandex_key&query=$str&lr=213&page=$cnt" );

    if ( $rsp->is_success ) {
      my $xml = $rsp->decoded_content;
      if ( $xml =~ m/error/ ) {
        $self->_clear_console( $progress );
        say 'ошибка поиска';
        my ( $err ) = $xml =~ m/\<error.*?\>(.*?)\<\/error\>/;
        say $err;
        return;
      }

      my @urls = $xml =~ m/\<url\>(.*?)\<\/url\>/g;
      push( @$pages, map( { { url => $_, level => 1, }; } @urls ) );
    }
    else {
      die $rsp->status_line;
    }

    $self->_clear_console( $progress );
    $progress = ( $cnt + 1 ) . " из $limit";
    print $progress;
  }

  $self->_clear_console( $progress );
  say 'ok';

  return $pages;
}

sub _get_text {
  my __PACKAGE__ $self = shift;
  my $site = shift;

  my $ua = $self->{ua};
  my $pps_limit = $self->{pps_limit};

  my $txt = '';
  my $progress = '';
  my $cnt = 0;

  my $hr = HTML::Restrict->new();

  my $pages = [ $site ];

  foreach my $page ( @$pages ) {
    $cnt++;

    my $rsp = $ua->get( $page->{url} );

    next unless $rsp->is_success;

    my $html = $rsp->decoded_content;
    next unless $html;

    if ( $page->{level} <= 2 && scalar( @$pages ) < $pps_limit ) {
      push @$pages, $self->_get_sub_pages( $page->{url}, $page->{level}, $html, scalar( @$pages ) );
    }

    my $cleared_html = $hr->process( $html );
    next unless $cleared_html;

    $txt .= $cleared_html;

    $self->_clear_console( $progress );
    $progress = "$cnt из " . scalar( @$pages );
    print $progress;
  }

  $self->_clear_console( $progress );

  $txt = decode_utf8( $txt );

  return $txt;
}

sub _get_sentences {
  my __PACKAGE__ $self = shift;
  my $text = shift;

  $text =~ s/\&ndash\;/-/g;
  $text =~ s/\&nbsp\;/-/g;
  $text =~ s/\s+/ /g;

  my @sentences = $text =~ m/([АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ][^.|!АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ?]{20,140})/gs;

  return \@sentences;
}

sub _clear_console {
  my __PACKAGE__ $self = shift;
  my $str = shift;

  my $back = "\b" x length $str;
  print $back;
  $back = " " x length $str;
  print $back;
  $back = "\b" x length $str;
  print $back;

  return;
}

sub _get_sub_pages {
  my __PACKAGE__ $self = shift;
  my $url = shift;
  my $level = shift;
  my $html = shift;
  my $count = shift;

  my $pages_processed = $self->{pages_processed};
  my $pps_limit = $self->{pps_limit};

  my @pages = ();

  my ( $domain ) = $url =~ m/^(http.?\:\/\/.+?\/)/;

  my @urls = $html =~ m/href\=\"(.*?)\"/g;

  foreach my $surl ( @urls ) {
    next if $surl =~ m/^mailto\:/;
    next if $surl =~ m/^ftp\:/;

    if ( $surl =~ m/^http/ ) {
      next unless $surl =~ m/^$domain/;

      next if $pages_processed->{ $surl };
      $pages_processed->{ $surl } = 1;

      push @pages, { url => $surl, level => $level + 1, };
      $count++;
    }
    else {
      $surl =~ s/\///;

      next if $pages_processed->{ "$domain$surl" };
      $pages_processed->{ "$domain$surl" } = 1;

      push @pages, { url => "$domain$surl", level => $level + 1, };
      $count++;
    }

    last if $count >= $pps_limit;
  }

  return @pages;
}

sub _get_anchors {
  my __PACKAGE__ $self = shift;
  my $sites = shift;

  my $sites_limit = $self->{sites_limit};
  my $anchors = { 'ТВО' => {}, 'ТВРО' => {}, 'ТВР' => {}, 'НВР' => {}, 'НВРО' => {}, };

  for ( my $cnt = 1; $cnt <= scalar( @$sites ); $cnt++ ) {
    last if $cnt > $sites_limit;

    print "Обработка сайта $cnt...";

    my $txt = $self->_get_text( $sites->[ $cnt - 1 ] );
    my $sentences = $self->_get_sentences( $txt );

    my $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvo' );
    map { $anchors->{ 'ТВО' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvro' );
    map { $anchors->{ 'ТВРО' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'tvr' );
    map { $anchors->{ 'ТВР' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'nvr' );
    map { $anchors->{ 'НВР' }->{ $_ } = 1; } @$curr_anchors;

    $curr_anchors = $self->_get_anchors_from_sentences( clone( $sentences ), 'nvro' );
    map { $anchors->{ 'НВРО' }->{ $_ } = 1; } @$curr_anchors;

    say 'ok';
  }

  return $anchors;
}

sub _get_anchors_from_sentences {
  my __PACKAGE__ $self = shift;
  my $sentences = shift;
  my $type = shift;

  my $keyword = $self->{keyword};

  my $anchors = {};

  foreach my $sent ( @$sentences ) {
    if ( $type eq 'tvo' ) {
      next unless $sent =~ m/$keyword/i;

      $sent =~ s/($keyword)/\#a\#$1\#\/a\#/i;

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'tvro' ) {
      next unless $sent =~ m/($keyword)/;
      my $found = $1;

      if ( $sent =~ m/\s.{4,20}\s\Q$found\E/i ) {
        $sent =~ s/\s(.{4,20}\s\Q$found\E)/ \#a\#$1/i;
      }
      elsif ( $sent =~ m/^.{0,20}\Q$found\E/i ) {
        $sent =~ s/^(.{0,20}\Q$found\E)/\#a\#$1/i;
      }

      if ( $sent =~ m/\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20}\s/i ) {
        $sent =~ s/(\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20})\s/$1\#\/a\# /i;
      }
      elsif ( $sent =~ m/\Q$found\E.{0,20}$/i ) {
        $sent =~ s/(\Q$found\E.{0,20})$/$1\#\/a\#/i;
      }

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'tvr' ) {
      next unless $sent =~ m/$keyword/i;

      $sent = "#a#$sent#/a#";

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'nvro' ) {
      my @words = split /\s/, $keyword;
      foreach ( @words ) {
        next if length $_ < 4;
        $_ =~ s/\S{3}$/\\S\{1\,3\}/;
      }

      my $keyword_re = join '.{1,20}', @words;

      next unless $sent =~ m/($keyword_re)/;
      my $found = $1;

      next if length( $found ) < length( $keyword ) + 7;

      @words = split /\s/, $keyword;
      my $skip = 1;
      foreach ( @words ) {
        next if length $_ < 4;
        if ( $found !~ m/$_/ ) {
          $skip = 0;
          last;
        }
      }

      next if $skip;

      if ( $sent =~ m/\s.{4,20}\s\Q$found\E/i ) {
        $sent =~ s/\s(.{4,20}\s\Q$found\E)/ \#a\#$1/i;
      }
      elsif ( $sent =~ m/^.{0,20}\Q$found\E/i ) {
        $sent =~ s/^(.{0,20}\Q$found\E)/\#a\#$1/i;
      }

      if ( $sent =~ m/\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20}\s/i ) {
        $sent =~ s/(\Q$found\E[^АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ\w].{4,20})\s/$1\#\/a\# /i;
      }
      elsif ( $sent =~ m/\Q$found\E.{0,20}$/i ) {
        $sent =~ s/(\Q$found\E.{0,20})$/$1\#\/a\#/i;
      }

      $anchors->{ $sent } = 1;
    }
    elsif ( $type eq 'nvr' ) {
      my @words = split /\s/, $keyword;
      foreach ( @words ) {
        next if length $_ < 4;
        $_ =~ s/\S{3}$/\\S\{1\,3\}/;
      }

      my $keyword_re = join '.{1,20}', @words;

      next unless $sent =~ m/($keyword_re)/;
      my $found = $1;

      next if length( $found ) < length( $keyword ) + 7;

      @words = split /\s/, $keyword;
      my $skip = 1;
      foreach ( @words ) {
        next if length $_ < 4;
        if ( $found !~ m/$_/ ) {
          $skip = 0;
          last;
        }
      }

      next if $skip;

      $sent = "#a#$sent#/a#";

      $anchors->{ $sent } = 1;
    }
  }

  return [ keys %$anchors ];
}

1;

__END__

=head1 NAME

anchors.pl - генератор текстов анкоров

=head1 SYNOPSIS

anchors.pl [параметры]

 Обязательные параметры:
   -k, --keyword <ключевая фраза>        ключевая фраза для генерации
   -f, --file <имя файла>                файл для сохранения текстов
   -y, --yandex_key <ключ>               ключ в Yandex.XML
   -u, --yandex_user <пользователь>      пользователь в Yandex.XML

 Не обязательные параметры:
   --sites <количество>                  количество сайтов для обработки, по умолчанию - 50
   --pages <количество>                  лимит количества страниц для одного сайта, по умолчанию - 200

 Дополнительные параметры:
   -v, --version                         показать версию

=head1 DESCRIPTION

Генератор текстов анкоров

=head1 AUTHOR

Kaktus, E<lt>kak-tus@mail.ruE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013-2014 Kaktus

=cut
