package krakken.io

case class Host(alias: String, ip: String){
  def asService:Option[Service]= {
    getContainerLink(alias)
  }
}
